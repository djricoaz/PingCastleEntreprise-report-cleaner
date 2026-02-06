#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PingCastle Database Maintenance
Monthly Retention Cleanup (based on report Generation / "Last report")

CUSTOMER NEED
- Weekly scans for years
- Keep ALL reports for the last N days (e.g. 1 year)
- For reports OLDER than N days: keep ONLY ONE report per month per domain
  (the latest report of that month based on Generation / "Last report")
- Dry-run by default, optional archive, optional delete

AUTHOR = Karim AZZOUZI
VENDOR = Netwrix Corporation
"""

from __future__ import annotations

import base64
import csv
import datetime as dt
import getpass
import json
import os
import platform
import traceback
import zipfile
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Iterable

# -------------------------
# Dependencies
# -------------------------
try:
    import pyodbc  # type: ignore
except Exception:
    print("Missing dependency: pyodbc")
    raise

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.prompt import Prompt, Confirm
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
    from rich.align import Align
    from rich.text import Text
    from rich.rule import Rule
except Exception:
    print("Missing dependency: rich")
    raise

console = Console()

# -------------------------
# i18n
# -------------------------
LANGS = ["EN", "FR", "IT", "DE", "ES"]

T: Dict[str, Dict[str, str]] = {
    "EN": {
        "title": "PingCastle Database Maintenance",
        "subtitle": "Monthly Retention Cleanup (Generation / Last report)",
        "tagline": "Keep everything recent • Keep 1 per month per domain for old reports • Optional archive/delete",
        "author": "AUTHOR = Karim AZZOUZI",
        "vendor": "VENDOR = Netwrix Corporation",

        "language": "Language",
        "invalid": "Invalid input, please retry.",
        "press_enter": "Press Enter to continue",

        "step": "Step",
        "sql_conn": "SQL Server connection",
        "odbc_driver": "Installed ODBC drivers (SQL Server)",
        "select_driver": "Select driver #",

        "host": "SQL Server host",
        "instance": "Instance name (blank for default)",
        "port": "TCP port",
        "db": "Database name",
        "auth": "Authentication [windows/sql]",
        "user": "SQL username",
        "pwd": "SQL password",
        "encrypt": "Encrypt connection?",
        "trust": "Trust server certificate?",
        "testing": "Testing connection...",
        "test_ok": "Connection OK.",
        "test_fail": "Connection failed.",
        "retry": "Retry connection settings?",

        "detect_reports": "Detecting PingCastle Reports table",
        "detect_children": "Detecting dependent tables (FK to Reports)",
        "reports_found": "Reports table found",
        "children_found": "Dependent tables found",
        "using_domain_name": "Domain name column detected",

        "retention": "Retention settings",
        "choose_retention": "Choose retention threshold",
        "ret_1y": "1) 1 year (365 days)",
        "ret_6m": "2) 6 months (183 days)",
        "ret_custom": "3) Custom days",
        "ret_prompt": "Selection [1/2/3]",
        "custom_days": "Enter retention days",

        "logic_title": "Customer rule",
        "logic_body": (
            "Reference date = Generation (aka 'Last report').\n"
            "• Reports newer than the threshold: KEEP ALL\n"
            "• Reports older than the threshold: KEEP ONLY the latest report per month per domain\n"
            "  and REMOVE the extra weekly reports."
        ),

        "plan": "Building retention plan",
        "loading_reports": "Loading reports from DB",
        "summary": "Retention Summary",
        "total": "Total reports",
        "cutoff": "Cutoff (UTC)",
        "old": "Reports older than threshold (Generation)",
        "keep_recent": "Reports kept (recent)",
        "keep_monthly": "Reports kept (monthly for old)",
        "extras": "Reports to remove (extras)",

        "per_domain": "Top domains with most removals",
        "domain": "Domain",
        "remove_count": "To remove",
        "kept_old_monthly": "Kept monthly (old)",

        "export_plan": "Exporting plan (CSV)",
        "plan_folder": "Plan folder",
        "plan_exported": "Plan exported",

        "preview_delete": "Preview: first rows to remove",
        "no_extras": "Nothing to remove. Exiting.",
        "safe_default_archive": "Default archive directory",
        "action_old": "For reports older than threshold: action [archive/delete]",
        "archive_dir": "Archive directory",
        "archive_fmt": "Archive format [jsonl/csv]",
        "zip_q": "Create ZIP archive?",
        "dryrun_prep": "Dry-run: preparing delete set (no data changed)",
        "dryrun_table": "Dry-run: impacted rows per table",
        "dryrun_ready": "Dry-run ready. Reports to remove",
        "apply": "Apply changes now? (Dry-run is default)",
        "final_confirm": "Final confirmation: this will DELETE data. Continue?",

        "archiving": "Archiving selected reports",
        "archive_created": "Archive created",
        "zip_created": "Archive ZIP created",

        "deleting": "Deleting dependent tables and reports",
        "done": "Done.",
        "dryrun_only": "Dry-run only. No changes applied.",
        "cancelled": "Cancelled. No deletion applied.",
        "logfile": "Log file",
        "err": "ERROR",
        "connected_to": "Connected to",
    },

    "FR": {
        "title": "Maintenance Base de Données PingCastle",
        "subtitle": "Nettoyage Rétention Mensuelle (Génération / Dernier rapport)",
        "tagline": "Tout garder en récent • 1/mois/domaine pour l’ancien • Archivage/Suppression optionnels",
        "author": "AUTHOR = Karim AZZOUZI",
        "vendor": "VENDOR = Netwrix Corporation",

        "language": "Langue",
        "invalid": "Entrée invalide, merci de réessayer.",
        "press_enter": "Appuyez sur Entrée pour continuer",

        "step": "Étape",
        "sql_conn": "Connexion SQL Server",
        "odbc_driver": "Drivers ODBC installés (SQL Server)",
        "select_driver": "Choisir le driver #",

        "host": "Hôte SQL Server",
        "instance": "Nom d’instance (vide = instance par défaut)",
        "port": "Port TCP",
        "db": "Nom de la base",
        "auth": "Authentification [windows/sql]",
        "user": "Utilisateur SQL",
        "pwd": "Mot de passe SQL",
        "encrypt": "Chiffrer la connexion ?",
        "trust": "Faire confiance au certificat serveur ?",
        "testing": "Test de connexion...",
        "test_ok": "Connexion OK.",
        "test_fail": "Échec de connexion.",
        "retry": "Recommencer la saisie de connexion ?",

        "detect_reports": "Détection de la table Reports PingCastle",
        "detect_children": "Détection des tables dépendantes (FK vers Reports)",
        "reports_found": "Table Reports détectée",
        "children_found": "Tables dépendantes détectées",
        "using_domain_name": "Colonne de nom de domaine détectée",

        "retention": "Paramètres de rétention",
        "choose_retention": "Choisissez le seuil de conservation",
        "ret_1y": "1) 1 an (365 jours)",
        "ret_6m": "2) 6 mois (183 jours)",
        "ret_custom": "3) Jours personnalisés",
        "ret_prompt": "Sélection [1/2/3]",
        "custom_days": "Entrez le nombre de jours de conservation",

        "logic_title": "Règle client",
        "logic_body": (
            "Date de référence = Génération (aka 'Dernier rapport').\n"
            "• Rapports plus récents que le seuil : TOUT CONSERVER\n"
            "• Rapports plus anciens que le seuil : ne conserver que le plus récent du mois par domaine\n"
            "  et supprimer les rapports hebdomadaires en trop."
        ),

        "plan": "Construction du plan de rétention",
        "loading_reports": "Chargement des rapports depuis la DB",
        "summary": "Résumé de Rétention",
        "total": "Total des rapports",
        "cutoff": "Seuil (UTC)",
        "old": "Rapports plus anciens que le seuil (Génération)",
        "keep_recent": "Rapports conservés (récents)",
        "keep_monthly": "Rapports conservés (mensuels pour anciens)",
        "extras": "Rapports à supprimer (extras)",

        "per_domain": "Top domaines avec le plus de suppressions",
        "domain": "Domaine",
        "remove_count": "À supprimer",
        "kept_old_monthly": "Mensuels conservés (anciens)",

        "export_plan": "Export du plan (CSV)",
        "plan_folder": "Dossier du plan",
        "plan_exported": "Plan exporté",

        "preview_delete": "Aperçu : premières lignes à supprimer",
        "no_extras": "Rien à supprimer. Fin.",
        "safe_default_archive": "Dossier d’archive par défaut",
        "action_old": "Pour les rapports plus anciens que le seuil : action [archive/delete]",
        "archive_dir": "Dossier d’archivage",
        "archive_fmt": "Format d’archive [jsonl/csv]",
        "zip_q": "Créer une archive ZIP ?",
        "dryrun_prep": "Dry-run : préparation de la suppression (aucune donnée modifiée)",
        "dryrun_table": "Dry-run : lignes impactées par table",
        "dryrun_ready": "Dry-run prêt. Rapports à supprimer",
        "apply": "Appliquer maintenant ? (Dry-run par défaut)",
        "final_confirm": "Confirmation finale : cela SUPPRIME des données. Continuer ?",

        "archiving": "Archivage des rapports sélectionnés",
        "archive_created": "Archive créée",
        "zip_created": "Archive ZIP créée",

        "deleting": "Suppression des tables dépendantes et des rapports",
        "done": "Terminé.",
        "dryrun_only": "Dry-run uniquement. Aucun changement appliqué.",
        "cancelled": "Annulé. Aucune suppression appliquée.",
        "logfile": "Fichier log",
        "err": "ERREUR",
        "connected_to": "Connecté à",
    },

    "IT": {
        "title": "Manutenzione Database PingCastle",
        "subtitle": "Pulizia Retention Mensile (Generazione / Ultimo report)",
        "tagline": "Conserva il recente • 1/mese/dominio per il vecchio • Archiviazione/Eliminazione opzionali",
        "author": "AUTHOR = Karim AZZOUZI",
        "vendor": "VENDOR = Netwrix Corporation",

        "language": "Lingua",
        "invalid": "Input non valido, riprova.",
        "press_enter": "Premi Invio per continuare",

        "step": "Passo",
        "sql_conn": "Connessione SQL Server",
        "odbc_driver": "Driver ODBC installati (SQL Server)",
        "select_driver": "Seleziona driver #",

        "host": "Host SQL Server",
        "instance": "Nome istanza (vuoto = predefinita)",
        "port": "Porta TCP",
        "db": "Nome database",
        "auth": "Autenticazione [windows/sql]",
        "user": "Username SQL",
        "pwd": "Password SQL",
        "encrypt": "Crittografare la connessione?",
        "trust": "Considerare attendibile il certificato del server?",
        "testing": "Test connessione...",
        "test_ok": "Connessione OK.",
        "test_fail": "Connessione fallita.",
        "retry": "Riprovare impostazioni di connessione?",

        "detect_reports": "Rilevamento tabella Reports PingCastle",
        "detect_children": "Rilevamento tabelle dipendenti (FK verso Reports)",
        "reports_found": "Tabella Reports trovata",
        "children_found": "Tabelle dipendenti trovate",
        "using_domain_name": "Colonna nome dominio rilevata",

        "retention": "Impostazioni retention",
        "choose_retention": "Scegli la soglia di retention",
        "ret_1y": "1) 1 anno (365 giorni)",
        "ret_6m": "2) 6 mesi (183 giorni)",
        "ret_custom": "3) Giorni personalizzati",
        "ret_prompt": "Selezione [1/2/3]",
        "custom_days": "Inserisci i giorni di retention",

        "logic_title": "Regola cliente",
        "logic_body": (
            "Data di riferimento = Generazione (aka 'Ultimo report').\n"
            "• Report più recenti della soglia: MANTIENI TUTTO\n"
            "• Report più vecchi della soglia: mantieni SOLO l'ultimo report del mese per dominio\n"
            "  ed elimina gli extra settimanali."
        ),

        "plan": "Creazione piano di retention",
        "loading_reports": "Caricamento report dal DB",
        "summary": "Riepilogo Retention",
        "total": "Report totali",
        "cutoff": "Soglia (UTC)",
        "old": "Report più vecchi della soglia (Generazione)",
        "keep_recent": "Report mantenuti (recenti)",
        "keep_monthly": "Report mantenuti (mensili per vecchi)",
        "extras": "Report da rimuovere (extra)",

        "per_domain": "Top domini con più rimozioni",
        "domain": "Dominio",
        "remove_count": "Da rimuovere",
        "kept_old_monthly": "Mensili mantenuti (vecchi)",

        "export_plan": "Esportazione piano (CSV)",
        "plan_folder": "Cartella piano",
        "plan_exported": "Piano esportato",

        "preview_delete": "Anteprima: prime righe da rimuovere",
        "no_extras": "Niente da rimuovere. Uscita.",
        "safe_default_archive": "Directory archivio predefinita",
        "action_old": "Per i report più vecchi della soglia: azione [archive/delete]",
        "archive_dir": "Directory archivio",
        "archive_fmt": "Formato archivio [jsonl/csv]",
        "zip_q": "Creare archivio ZIP?",
        "dryrun_prep": "Dry-run: preparazione set di eliminazione (nessuna modifica)",
        "dryrun_table": "Dry-run: righe impattate per tabella",
        "dryrun_ready": "Dry-run pronto. Report da rimuovere",
        "apply": "Applicare ora? (Dry-run è predefinito)",
        "final_confirm": "Conferma finale: questo ELIMINERÀ dati. Continuare?",

        "archiving": "Archiviazione report selezionati",
        "archive_created": "Archivio creato",
        "zip_created": "ZIP archivio creato",

        "deleting": "Eliminazione tabelle dipendenti e report",
        "done": "Fatto.",
        "dryrun_only": "Solo dry-run. Nessuna modifica applicata.",
        "cancelled": "Annullato. Nessuna eliminazione applicata.",
        "logfile": "File log",
        "err": "ERRORE",
        "connected_to": "Connesso a",
    },

    "DE": {
        "title": "PingCastle Datenbankwartung",
        "subtitle": "Monatliche Retention-Bereinigung (Generation / Letzter Report)",
        "tagline": "Aktuelles behalten • Für alte Reports 1/Monat/Domäne • Optional archivieren/löschen",
        "author": "AUTHOR = Karim AZZOUZI",
        "vendor": "VENDOR = Netwrix Corporation",

        "language": "Sprache",
        "invalid": "Ungültige Eingabe, bitte erneut versuchen.",
        "press_enter": "Drücken Sie Enter, um fortzufahren",

        "step": "Schritt",
        "sql_conn": "SQL Server Verbindung",
        "odbc_driver": "Installierte ODBC-Treiber (SQL Server)",
        "select_driver": "Treiber auswählen #",

        "host": "SQL Server Host",
        "instance": "Instanzname (leer = Standard)",
        "port": "TCP-Port",
        "db": "Datenbankname",
        "auth": "Authentifizierung [windows/sql]",
        "user": "SQL Benutzername",
        "pwd": "SQL Passwort",
        "encrypt": "Verbindung verschlüsseln?",
        "trust": "Serverzertifikat vertrauen?",
        "testing": "Verbindung wird getestet...",
        "test_ok": "Verbindung OK.",
        "test_fail": "Verbindung fehlgeschlagen.",
        "retry": "Verbindungseinstellungen erneut eingeben?",

        "detect_reports": "Erkennung der PingCastle Reports-Tabelle",
        "detect_children": "Erkennung abhängiger Tabellen (FK zu Reports)",
        "reports_found": "Reports-Tabelle gefunden",
        "children_found": "Abhängige Tabellen gefunden",
        "using_domain_name": "Spalte für Domänennamen erkannt",

        "retention": "Retention-Einstellungen",
        "choose_retention": "Retention-Schwelle wählen",
        "ret_1y": "1) 1 Jahr (365 Tage)",
        "ret_6m": "2) 6 Monate (183 Tage)",
        "ret_custom": "3) Benutzerdefinierte Tage",
        "ret_prompt": "Auswahl [1/2/3]",
        "custom_days": "Retention-Tage eingeben",

        "logic_title": "Kundenregel",
        "logic_body": (
            "Referenzdatum = Generation (aka 'Letzter Report').\n"
            "• Reports neuer als die Schwelle: ALLE BEHALTEN\n"
            "• Reports älter als die Schwelle: pro Monat und Domäne nur den neuesten Report behalten\n"
            "  und die übrigen (wöchentlichen) löschen."
        ),

        "plan": "Retention-Plan wird erstellt",
        "loading_reports": "Reports aus der DB werden geladen",
        "summary": "Retention-Zusammenfassung",
        "total": "Reports gesamt",
        "cutoff": "Schwelle (UTC)",
        "old": "Reports älter als die Schwelle (Generation)",
        "keep_recent": "Behalten (neu)",
        "keep_monthly": "Behalten (monatlich für alt)",
        "extras": "Zu entfernen (Extras)",

        "per_domain": "Top-Domänen mit den meisten Löschungen",
        "domain": "Domäne",
        "remove_count": "Zu entfernen",
        "kept_old_monthly": "Monatlich behalten (alt)",

        "export_plan": "Plan wird exportiert (CSV)",
        "plan_folder": "Plan-Ordner",
        "plan_exported": "Plan exportiert",

        "preview_delete": "Vorschau: erste Zeilen zum Entfernen",
        "no_extras": "Nichts zu entfernen. Beenden.",
        "safe_default_archive": "Standard-Archivverzeichnis",
        "action_old": "Für Reports älter als die Schwelle: Aktion [archive/delete]",
        "archive_dir": "Archivverzeichnis",
        "archive_fmt": "Archivformat [jsonl/csv]",
        "zip_q": "ZIP-Archiv erstellen?",
        "dryrun_prep": "Dry-run: Delete-Set wird vorbereitet (keine Änderungen)",
        "dryrun_table": "Dry-run: betroffene Zeilen pro Tabelle",
        "dryrun_ready": "Dry-run bereit. Reports zum Entfernen",
        "apply": "Jetzt anwenden? (Dry-run ist Standard)",
        "final_confirm": "Letzte Bestätigung: Daten werden GELÖSCHT. Fortfahren?",

        "archiving": "Ausgewählte Reports werden archiviert",
        "archive_created": "Archiv erstellt",
        "zip_created": "ZIP-Archiv erstellt",

        "deleting": "Abhängige Tabellen und Reports werden gelöscht",
        "done": "Fertig.",
        "dryrun_only": "Nur Dry-run. Keine Änderungen angewendet.",
        "cancelled": "Abgebrochen. Keine Löschung durchgeführt.",
        "logfile": "Logdatei",
        "err": "FEHLER",
        "connected_to": "Verbunden mit",
    },

    "ES": {
        "title": "Mantenimiento de Base de Datos PingCastle",
        "subtitle": "Limpieza de Retención Mensual (Generación / Último informe)",
        "tagline": "Conservar lo reciente • 1/mes/dominio para lo antiguo • Archivar/Eliminar opcional",
        "author": "AUTHOR = Karim AZZOUZI",
        "vendor": "VENDOR = Netwrix Corporation",

        "language": "Idioma",
        "invalid": "Entrada no válida, inténtalo de nuevo.",
        "press_enter": "Pulsa Enter para continuar",

        "step": "Paso",
        "sql_conn": "Conexión SQL Server",
        "odbc_driver": "Drivers ODBC instalados (SQL Server)",
        "select_driver": "Selecciona driver #",

        "host": "Host SQL Server",
        "instance": "Nombre de instancia (vacío = por defecto)",
        "port": "Puerto TCP",
        "db": "Nombre de la base",
        "auth": "Autenticación [windows/sql]",
        "user": "Usuario SQL",
        "pwd": "Contraseña SQL",
        "encrypt": "¿Cifrar la conexión?",
        "trust": "¿Confiar en el certificado del servidor?",
        "testing": "Probando conexión...",
        "test_ok": "Conexión OK.",
        "test_fail": "Conexión fallida.",
        "retry": "¿Reintentar configuración de conexión?",

        "detect_reports": "Detectando tabla Reports de PingCastle",
        "detect_children": "Detectando tablas dependientes (FK a Reports)",
        "reports_found": "Tabla Reports encontrada",
        "children_found": "Tablas dependientes encontradas",
        "using_domain_name": "Columna de nombre de dominio detectada",

        "retention": "Configuración de retención",
        "choose_retention": "Elige el umbral de retención",
        "ret_1y": "1) 1 año (365 días)",
        "ret_6m": "2) 6 meses (183 días)",
        "ret_custom": "3) Días personalizados",
        "ret_prompt": "Selección [1/2/3]",
        "custom_days": "Introduce los días de retención",

        "logic_title": "Regla del cliente",
        "logic_body": (
            "Fecha de referencia = Generación (aka 'Último informe').\n"
            "• Informes más recientes que el umbral: CONSERVAR TODO\n"
            "• Informes más antiguos que el umbral: conservar SOLO el informe más reciente del mes por dominio\n"
            "  y eliminar los informes semanales extra."
        ),

        "plan": "Construyendo plan de retención",
        "loading_reports": "Cargando informes desde la BD",
        "summary": "Resumen de Retención",
        "total": "Total de informes",
        "cutoff": "Umbral (UTC)",
        "old": "Informes más antiguos que el umbral (Generación)",
        "keep_recent": "Informes conservados (recientes)",
        "keep_monthly": "Informes conservados (mensuales para antiguos)",
        "extras": "Informes a eliminar (extras)",

        "per_domain": "Dominios con más eliminaciones",
        "domain": "Dominio",
        "remove_count": "A eliminar",
        "kept_old_monthly": "Mensuales conservados (antiguos)",

        "export_plan": "Exportando plan (CSV)",
        "plan_folder": "Carpeta del plan",
        "plan_exported": "Plan exportado",

        "preview_delete": "Vista previa: primeras filas a eliminar",
        "no_extras": "Nada que eliminar. Saliendo.",
        "safe_default_archive": "Directorio de archivo por defecto",
        "action_old": "Para informes más antiguos que el umbral: acción [archive/delete]",
        "archive_dir": "Directorio de archivo",
        "archive_fmt": "Formato de archivo [jsonl/csv]",
        "zip_q": "¿Crear archivo ZIP?",
        "dryrun_prep": "Dry-run: preparando conjunto de eliminación (sin cambios)",
        "dryrun_table": "Dry-run: filas impactadas por tabla",
        "dryrun_ready": "Dry-run listo. Informes a eliminar",
        "apply": "¿Aplicar ahora? (Dry-run por defecto)",
        "final_confirm": "Confirmación final: esto BORRARÁ datos. ¿Continuar?",

        "archiving": "Archivando informes seleccionados",
        "archive_created": "Archivo creado",
        "zip_created": "ZIP creado",

        "deleting": "Eliminando tablas dependientes e informes",
        "done": "Hecho.",
        "dryrun_only": "Solo dry-run. No se aplicaron cambios.",
        "cancelled": "Cancelado. No se aplicó ninguna eliminación.",
        "logfile": "Archivo log",
        "err": "ERROR",
        "connected_to": "Conectado a",
    },
}


def tr(lang: str, key: str) -> str:
    return T.get(lang, T["EN"]).get(key, T["EN"].get(key, key))


# -------------------------
# Logging
# -------------------------
def now_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")


def log_path_default() -> str:
    return f"PingCastleMaintenance-{now_stamp()}.log"


def log_write(logfile: str, msg: str) -> None:
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")


def ok(msg: str) -> None:
    console.print(f"[bold green]✔[/bold green] {msg}")


def warn(msg: str) -> None:
    console.print(f"[bold yellow]⚠[/bold yellow] {msg}")


def info(msg: str) -> None:
    console.print(f"[bold cyan]▶[/bold cyan] {msg}")


def err(msg: str) -> None:
    console.print(f"[bold red]✖[/bold red] {msg}")


# -------------------------
# SQL structures
# -------------------------
@dataclass
class ReportsTable:
    schema: str
    table: str

    @property
    def fq(self) -> str:
        return f"{q(self.schema)}.{q(self.table)}"


@dataclass
class FKRef:
    child_schema: str
    child_table: str
    child_column: str

    @property
    def child_fq(self) -> str:
        return f"{q(self.child_schema)}.{q(self.child_table)}"


def q(identifier: str) -> str:
    return "[" + identifier.replace("]", "]]") + "]"


# -------------------------
# Connection builder
# -------------------------
def list_odbc_sqlserver_drivers() -> List[str]:
    drivers = pyodbc.drivers()
    preferred: List[str] = []
    others: List[str] = []
    for d in drivers:
        if "SQL Server" in d or "ODBC Driver" in d or "Native Client" in d:
            if "ODBC Driver 18" in d:
                preferred.append(d)
            elif "ODBC Driver 17" in d:
                preferred.append(d)
            else:
                others.append(d)
    ordered = preferred + [d for d in others if d not in preferred]
    return ordered


def choose_driver(lang: str) -> str:
    drivers = list_odbc_sqlserver_drivers()
    if not drivers:
        raise RuntimeError("No SQL Server ODBC driver found.")

    table = Table(title=tr(lang, "odbc_driver"), show_lines=True)
    table.add_column("#", justify="right", style="bold")
    table.add_column("Driver name", overflow="fold")
    for i, d in enumerate(drivers, 1):
        table.add_row(str(i), d)
    console.print(table)

    default_idx = 1
    for i, d in enumerate(drivers, 1):
        if "ODBC Driver 18 for SQL Server" in d:
            default_idx = i
            break
        if "ODBC Driver 17 for SQL Server" in d:
            default_idx = i

    while True:
        s = Prompt.ask(tr(lang, "select_driver"), default=str(default_idx)).strip()
        if s.isdigit() and 1 <= int(s) <= len(drivers):
            return drivers[int(s) - 1]
        warn(tr(lang, "invalid"))


def build_server(host: str, instance: str, port: int) -> str:
    host = host.strip()
    instance = instance.strip()
    if instance:
        if port and port != 1433:
            return f"{host}\\{instance},{port}"
        return f"{host}\\{instance}"
    if port:
        return f"tcp:{host},{port}"
    return f"tcp:{host},1433"


def connect_sqlserver_interactive(lang: str, logfile: str) -> Tuple[pyodbc.Connection, str]:
    driver = choose_driver(lang)

    host = Prompt.ask(tr(lang, "host"), default=os.environ.get("COMPUTERNAME", "localhost"))
    instance = Prompt.ask(tr(lang, "instance"), default="").strip()
    port_s = Prompt.ask(tr(lang, "port"), default="1433")
    try:
        port = int(port_s.strip())
    except Exception:
        port = 1433

    db = Prompt.ask(tr(lang, "db"), default="PingCastleEnterprise")
    auth = Prompt.ask(tr(lang, "auth"), default="windows").strip().lower()
    if auth not in ("windows", "sql"):
        auth = "windows"

    encrypt = Confirm.ask(tr(lang, "encrypt"), default=True)
    trust = Confirm.ask(tr(lang, "trust"), default=True)

    uid = ""
    pwd = ""
    if auth == "sql":
        uid = Prompt.ask(tr(lang, "user"), default="")
        pwd = getpass.getpass(tr(lang, "pwd") + ": ")

    server = build_server(host, instance, port)

    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={db}",
        "Connection Timeout=30",
        "Application Name=PingCastleMaintenance",
    ]
    parts.append("Encrypt=yes" if encrypt else "Encrypt=no")
    parts.append(f"TrustServerCertificate={'yes' if trust else 'no'}")

    if auth == "windows":
        parts.append("Trusted_Connection=yes")
    else:
        parts.append(f"UID={uid}")
        parts.append(f"PWD={pwd}")

    conn_str = ";".join(parts) + ";"

    info(tr(lang, "testing"))
    conn = pyodbc.connect(conn_str, autocommit=False)
    ok(tr(lang, "test_ok"))
    ok(f"{tr(lang, 'connected_to')} {server}  |  DB={db}")
    log_write(logfile, f"[conn] OK server={server} db={db} auth={auth} driver={driver}")
    return conn, conn_str


# -------------------------
# Schema detection
# -------------------------
def detect_reports_table(cur) -> ReportsTable:
    cur.execute(
        """
        SELECT s.name AS schema_name, t.name AS table_name
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'Reports'
        """
    )
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError("Reports table not found.")

    best: Optional[ReportsTable] = None
    best_score = -1

    for r in rows:
        schema, table = r[0], r[1]
        cur.execute(
            """
            SELECT c.name
            FROM sys.columns c
            JOIN sys.tables t ON c.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ?
            """,
            schema, table,
        )
        cols = {x[0] for x in cur.fetchall()}
        score = 0
        for needed in ("ID", "DomainID", "ImportedDate", "RawData", "Generation"):
            if needed in cols:
                score += 5
        if score > best_score:
            best_score = score
            best = ReportsTable(schema=schema, table=table)

    if not best:
        raise RuntimeError("Unable to select Reports table.")
    return best


def detect_domains_table(cur) -> Tuple[str, str]:
    cur.execute(
        """
        SELECT s.name AS schema_name, t.name AS table_name
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'Domains'
        """
    )
    r = cur.fetchone()
    if not r:
        raise RuntimeError("Domains table not found.")
    return r[0], r[1]


def detect_report_name_source(cur) -> Tuple[str, str, str]:
    dom_schema, dom_table = detect_domains_table(cur)

    for candidate in ("Name", "NetBiosName"):
        cur.execute(
            """
            SELECT 1
            FROM sys.columns c
            JOIN sys.tables t ON c.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ? AND c.name = ?
            """,
            dom_schema, dom_table, candidate,
        )
        if cur.fetchone():
            return dom_schema, dom_table, candidate

    return dom_schema, dom_table, "ID"


def detect_dependent_tables(cur, reports: ReportsTable) -> List[FKRef]:
    cur.execute(
        """
        ;WITH fk AS (
            SELECT
                sch_child.name AS child_schema,
                t_child.name AS child_table,
                c_child.name AS child_column,
                sch_parent.name AS parent_schema,
                t_parent.name AS parent_table
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.tables t_child ON fkc.parent_object_id = t_child.object_id
            JOIN sys.schemas sch_child ON t_child.schema_id = sch_child.schema_id
            JOIN sys.columns c_child ON c_child.object_id = t_child.object_id AND c_child.column_id = fkc.parent_column_id
            JOIN sys.tables t_parent ON fkc.referenced_object_id = t_parent.object_id
            JOIN sys.schemas sch_parent ON t_parent.schema_id = sch_parent.schema_id
        )
        SELECT child_schema, child_table, child_column
        FROM fk
        WHERE parent_schema = ? AND parent_table = ?
        ORDER BY child_schema, child_table, child_column
        """,
        reports.schema, reports.table,
    )
    return [FKRef(child_schema=r[0], child_table=r[1], child_column=r[2]) for r in cur.fetchall()]


# -------------------------
# Retention logic (Generation / Last report)
# -------------------------
@dataclass
class ReportRow:
    id: int
    domain_id: int
    domain_name: str
    imported: dt.datetime
    generation: dt.datetime


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def normalize_dt(value) -> dt.datetime:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc)
    raise TypeError("Unsupported datetime value")


def month_key_from_generation(d: dt.datetime) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def load_reports(cur, reports: ReportsTable, dom_schema: str, dom_table: str, dom_name_col: str) -> List[ReportRow]:
    sql = f"""
    SELECT r.ID, r.DomainID, d.{q(dom_name_col)} AS DomainName, r.ImportedDate, r.Generation
    FROM {reports.fq} r
    JOIN {q(dom_schema)}.{q(dom_table)} d ON d.ID = r.DomainID
    ORDER BY r.Generation ASC, r.ID ASC
    """
    cur.execute(sql)
    out: List[ReportRow] = []
    for rid, did, dname, imported, gen in cur.fetchall():
        if gen is None:
            # Defensive: if Generation is NULL, skip (cannot apply customer rule safely).
            # Better: include it as "old" or "recent"? Here: skip + log later.
            continue
        out.append(
            ReportRow(
                id=int(rid),
                domain_id=int(did),
                domain_name=str(dname),
                imported=normalize_dt(imported),
                generation=normalize_dt(gen),
            )
        )
    return out


def compute_plan_monthly_over_cutoff(
    reports: List[ReportRow],
    cutoff_days: int,
) -> Tuple[dt.datetime, List[ReportRow], List[ReportRow], List[ReportRow]]:
    cutoff = utc_now() - dt.timedelta(days=cutoff_days)

    keep_recent: List[ReportRow] = []
    old: List[ReportRow] = []

    for r in reports:
        if r.generation >= cutoff:
            keep_recent.append(r)
        else:
            old.append(r)

    # For OLD reports: keep latest per (domain, month) based on Generation
    best: Dict[Tuple[int, str], ReportRow] = {}
    for r in old:
        k = (r.domain_id, month_key_from_generation(r.generation))
        cur_best = best.get(k)
        if cur_best is None or (r.generation, r.id) > (cur_best.generation, cur_best.id):
            best[k] = r

    keep_monthly = sorted(best.values(), key=lambda x: (x.generation, x.id))
    keep_ids = {r.id for r in keep_recent} | {r.id for r in keep_monthly}

    delete_extras = [r for r in old if r.id not in keep_ids]
    delete_extras.sort(key=lambda x: (x.generation, x.id))

    keep_recent.sort(key=lambda x: (x.generation, x.id))
    return cutoff, keep_recent, keep_monthly, delete_extras


# -------------------------
# Exports
# -------------------------
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def safe_default_archive_dir() -> str:
    return r"C:\PingCastleArchive"


def export_csv(path: str, rows: List[ReportRow]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["ID", "DomainID", "DomainName", "ImportedDateUTC", "GenerationUTC", "Month(Generation)"])
        for r in rows:
            w.writerow(
                [
                    r.id,
                    r.domain_id,
                    r.domain_name,
                    r.imported.isoformat(),
                    r.generation.isoformat(),
                    month_key_from_generation(r.generation),
                ]
            )


def export_all_plan(
    folder: str,
    reports_all: List[ReportRow],
    keep_recent: List[ReportRow],
    keep_monthly: List[ReportRow],
    delete_extras: List[ReportRow],
) -> Dict[str, str]:
    ensure_dir(folder)
    p_all = os.path.join(folder, "ALL_Reports.csv")
    p_recent = os.path.join(folder, "KEEP_Recent.csv")
    p_monthly = os.path.join(folder, "KEEP_Monthly.csv")
    p_delete = os.path.join(folder, "DELETE_Extras.csv")

    export_csv(p_all, reports_all)
    export_csv(p_recent, keep_recent)
    export_csv(p_monthly, keep_monthly)
    export_csv(p_delete, delete_extras)

    return {"ALL": p_all, "KEEP_Recent": p_recent, "KEEP_Monthly": p_monthly, "DELETE_Extras": p_delete}


def export_archive_jsonl(cur, reports_table: ReportsTable, report_ids: List[int], out_dir: str) -> str:
    ensure_dir(out_dir)
    out_file = os.path.join(out_dir, "ReportsArchive.jsonl")
    sql = f"SELECT ID, DomainID, ImportedDate, Generation, RawData FROM {reports_table.fq} WHERE ID = ?"

    with open(out_file, "w", encoding="utf-8") as f:
        for rid in report_ids:
            cur.execute(sql, rid)
            row = cur.fetchone()
            if not row:
                continue
            raw = row[4]
            raw_b64 = base64.b64encode(raw).decode("ascii") if raw is not None else None
            payload = {
                "ID": int(row[0]),
                "DomainID": int(row[1]),
                "ImportedDate": normalize_dt(row[2]).isoformat(),
                "Generation": normalize_dt(row[3]).isoformat() if row[3] else None,
                "RawDataBase64": raw_b64,
            }
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return out_file


def export_archive_csv(cur, reports_table: ReportsTable, report_ids: List[int], out_dir: str) -> str:
    ensure_dir(out_dir)
    out_file = os.path.join(out_dir, "ReportsArchive.csv")
    sql = f"SELECT ID, DomainID, ImportedDate, Generation FROM {reports_table.fq} WHERE ID = ?"

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["ID", "DomainID", "ImportedDateUTC", "GenerationUTC"])
        for rid in report_ids:
            cur.execute(sql, rid)
            row = cur.fetchone()
            if not row:
                continue
            w.writerow(
                [
                    int(row[0]),
                    int(row[1]),
                    normalize_dt(row[2]).isoformat(),
                    normalize_dt(row[3]).isoformat() if row[3] else "",
                ]
            )
    return out_file


def zip_folder(folder: str, zip_path: str) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(folder):
            for fn in files:
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, folder)
                z.write(full, rel)


# -------------------------
# Delete logic
# -------------------------
def create_delete_ids_temp(cur, ids: List[int]) -> None:
    cur.execute("IF OBJECT_ID('tempdb..#PC_DeleteIds') IS NOT NULL DROP TABLE #PC_DeleteIds;")
    cur.execute("CREATE TABLE #PC_DeleteIds (ID INT NOT NULL PRIMARY KEY);")
    if ids:
        cur.fast_executemany = True
        cur.executemany("INSERT INTO #PC_DeleteIds (ID) VALUES (?);", [(i,) for i in ids])


def update_domains_first_last(cur, reports: ReportsTable, dom_schema: str, dom_table: str) -> None:
    dom_fq = f"{q(dom_schema)}.{q(dom_table)}"
    sql = f"""
    ;WITH remaining AS (
        SELECT r.DomainID, r.ID, r.Generation
        FROM {reports.fq} r
        WHERE NOT EXISTS (SELECT 1 FROM #PC_DeleteIds d WHERE d.ID = r.ID)
    ),
    agg AS (
        SELECT DomainID,
               COUNT(1) AS Cnt,
               MIN(Generation) AS MinDt,
               MAX(Generation) AS MaxDt
        FROM remaining
        GROUP BY DomainID
    ),
    first_last AS (
        SELECT a.DomainID,
               a.Cnt,
               firstR.ID AS FirstReportID_New,
               lastR.ID AS LastReportID_New
        FROM agg a
        OUTER APPLY (
            SELECT TOP 1 ID
            FROM remaining r
            WHERE r.DomainID = a.DomainID AND r.Generation = a.MinDt
            ORDER BY r.ID ASC
        ) firstR
        OUTER APPLY (
            SELECT TOP 1 ID
            FROM remaining r
            WHERE r.DomainID = a.DomainID AND r.Generation = a.MaxDt
            ORDER BY r.ID DESC
        ) lastR
    )
    UPDATE d
        SET d.FirstReportID = fl.FirstReportID_New,
            d.LastReportID  = fl.LastReportID_New,
            d.NumberOfReport = fl.Cnt
    FROM {dom_fq} d
    LEFT JOIN first_last fl ON fl.DomainID = d.ID;
    """
    cur.execute(sql)


def delete_children_then_reports(cur, reports: ReportsTable, deps: List[FKRef]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for ref in deps:
        sql = f"DELETE FROM {ref.child_fq} WHERE {q(ref.child_column)} IN (SELECT ID FROM #PC_DeleteIds);"
        cur.execute(sql)
        counts[str(ref.child_fq)] = counts.get(str(ref.child_fq), 0) + cur.rowcount

    cur.execute(f"DELETE FROM {reports.fq} WHERE ID IN (SELECT ID FROM #PC_DeleteIds);")
    counts[str(reports.fq)] = counts.get(str(reports.fq), 0) + cur.rowcount
    return counts


def dryrun_counts(cur, reports: ReportsTable, deps: List[FKRef]) -> List[Tuple[str, int]]:
    rows: List[Tuple[str, int]] = []
    cur.execute(f"SELECT COUNT(1) FROM {reports.fq} WHERE ID IN (SELECT ID FROM #PC_DeleteIds);")
    rows.append((str(reports.fq), int(cur.fetchone()[0])))

    for ref in deps:
        cur.execute(f"SELECT COUNT(1) FROM {ref.child_fq} WHERE {q(ref.child_column)} IN (SELECT ID FROM #PC_DeleteIds);")
        rows.append((str(ref.child_fq), int(cur.fetchone()[0])))
    return rows


# -------------------------
# UI helpers
# -------------------------
def show_header(lang: str) -> None:
    title = Text(tr(lang, "title"), style="bold white")
    subtitle = Text(tr(lang, "subtitle"), style="bold cyan")
    tagline = Text(tr(lang, "tagline"), style="dim")
    author = Text(tr(lang, "author") + "    " + tr(lang, "vendor"), style="dim")
    sysline = Text(f"{platform.system()} {platform.release()} • Python {platform.python_version()}", style="dim")

    content = Text.assemble(title, "\n", subtitle, "\n", tagline, "\n", author, "\n", sysline)
    panel = Panel(Align.center(content), border_style="bright_cyan")
    console.print(panel)


def step_rule(lang: str, n: int, label: str) -> None:
    console.print(Rule(f"[bold cyan]{tr(lang, 'step')} {n}[/bold cyan] • {label}"))


def progress_run(label: str, func):
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as p:
        t = p.add_task(label, total=None)
        result = func()
        p.update(t, completed=1)
        return result


def show_summary(lang: str, total: int, cutoff: dt.datetime, old: int, keep_recent: int, keep_monthly: int, extras: int) -> None:
    tb = Table(title=tr(lang, "summary"), show_lines=True)
    tb.add_column("Metric")
    tb.add_column("Value", justify="right")

    tb.add_row(tr(lang, "total"), str(total))
    tb.add_row(tr(lang, "cutoff"), cutoff.isoformat())
    tb.add_row(tr(lang, "old"), str(old))
    tb.add_row(tr(lang, "keep_recent"), str(keep_recent))
    tb.add_row(tr(lang, "keep_monthly"), str(keep_monthly))
    tb.add_row(tr(lang, "extras"), str(extras))

    console.print(tb)


def show_dryrun_table(lang: str, rows: List[Tuple[str, int]]) -> None:
    tb = Table(title=tr(lang, "dryrun_table"), show_lines=True)
    tb.add_column("Table", overflow="fold")
    tb.add_column("Rows", justify="right")
    for name, cnt in rows:
        tb.add_row(name, str(cnt))
    console.print(tb)


def top_removals_by_domain(delete_extras: List[ReportRow], keep_monthly: List[ReportRow], topn: int = 10) -> List[Tuple[str, int, int]]:
    remove_counts: Dict[str, int] = {}
    kept_counts: Dict[str, int] = {}

    for r in delete_extras:
        remove_counts[r.domain_name] = remove_counts.get(r.domain_name, 0) + 1
    for r in keep_monthly:
        kept_counts[r.domain_name] = kept_counts.get(r.domain_name, 0) + 1

    merged = []
    for dom, cnt in remove_counts.items():
        merged.append((dom, cnt, kept_counts.get(dom, 0)))

    merged.sort(key=lambda x: x[1], reverse=True)
    return merged[:topn]


def show_domain_table(lang: str, rows: List[Tuple[str, int, int]]) -> None:
    tb = Table(title=tr(lang, "per_domain"), show_lines=True)
    tb.add_column(tr(lang, "domain"), overflow="fold")
    tb.add_column(tr(lang, "remove_count"), justify="right")
    tb.add_column(tr(lang, "kept_old_monthly"), justify="right")
    for dom, rm, kept in rows:
        tb.add_row(dom, str(rm), str(kept))
    console.print(tb)


def show_preview_delete(lang: str, delete_extras: List[ReportRow], limit: int = 12) -> None:
    tb = Table(title=tr(lang, "preview_delete"), show_lines=False)
    tb.add_column("ID", justify="right")
    tb.add_column("Domain", overflow="fold")
    tb.add_column("Generation (UTC)", overflow="fold")
    tb.add_column("Month", justify="center")

    for r in delete_extras[:limit]:
        tb.add_row(str(r.id), r.domain_name, r.generation.isoformat(), month_key_from_generation(r.generation))
    console.print(tb)


# -------------------------
# Main prompts
# -------------------------
def choose_language() -> str:
    while True:
        lang = Prompt.ask("Language [EN/FR/IT/DE/ES]", default="FR").strip().upper()
        if lang in LANGS:
            return lang
        console.print("Invalid language. Use EN/FR/IT/DE/ES.")


def choose_retention_days(lang: str) -> int:
    console.print(Panel(tr(lang, "retention"), border_style="cyan"))
    console.print(tr(lang, "ret_1y"))
    console.print(tr(lang, "ret_6m"))
    console.print(tr(lang, "ret_custom"))
    while True:
        s = Prompt.ask(tr(lang, "ret_prompt"), default="1").strip()
        if s == "1":
            return 365
        if s == "2":
            return 183
        if s == "3":
            d = Prompt.ask(tr(lang, "custom_days"), default="365").strip()
            if d.isdigit() and int(d) > 0:
                return int(d)
        warn(tr(lang, "invalid"))


def validate_writable_dir(path: str) -> bool:
    try:
        ensure_dir(path)
        testfile = os.path.join(path, f".write_test_{os.getpid()}.tmp")
        with open(testfile, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(testfile)
        return True
    except Exception:
        return False


# -------------------------
# Main
# -------------------------
def main() -> int:
    logfile = log_path_default()
    log_write(logfile, "=== PingCastle Maintenance START ===")

    lang = choose_language()
    show_header(lang)

    console.print(Panel(tr(lang, "logic_body"), title=tr(lang, "logic_title"), border_style="cyan"))

    step_rule(lang, 1, tr(lang, "sql_conn"))

    while True:
        try:
            conn, _ = connect_sqlserver_interactive(lang, logfile)
            break
        except Exception as e:
            err(tr(lang, "test_fail"))
            err(str(e))
            log_write(logfile, f"[conn] FAIL: {e}")
            if not Confirm.ask(tr(lang, "retry"), default=True):
                console.print(f"{tr(lang, 'logfile')}: {logfile}")
                return 2

    cur = conn.cursor()

    step_rule(lang, 2, tr(lang, "detect_reports"))
    reports = progress_run(tr(lang, "detect_reports"), lambda: detect_reports_table(cur))
    ok(f"{tr(lang, 'reports_found')}: {reports.fq}")
    log_write(logfile, f"[detect] Reports={reports.fq}")

    dom_schema, dom_table, dom_name_col = detect_report_name_source(cur)
    ok(f"{tr(lang, 'using_domain_name')}: {q(dom_schema)}.{q(dom_table)}.{q(dom_name_col)}")
    log_write(logfile, f"[detect] Domains={q(dom_schema)}.{q(dom_table)} namecol={dom_name_col}")

    step_rule(lang, 3, tr(lang, "detect_children"))
    deps = progress_run(tr(lang, "detect_children"), lambda: detect_dependent_tables(cur, reports))
    ok(f"{tr(lang, 'children_found')}: {len(deps)}")
    log_write(logfile, f"[detect] deps={len(deps)}")

    step_rule(lang, 4, tr(lang, "retention"))
    cutoff_days = choose_retention_days(lang)

    step_rule(lang, 5, tr(lang, "plan"))
    reports_all = progress_run(tr(lang, "loading_reports"), lambda: load_reports(cur, reports, dom_schema, dom_table, dom_name_col))
    cutoff, keep_recent, keep_monthly, delete_extras = compute_plan_monthly_over_cutoff(reports_all, cutoff_days)

    old_count = sum(1 for r in reports_all if r.generation < cutoff)

    show_summary(
        lang,
        total=len(reports_all),
        cutoff=cutoff,
        old=old_count,
        keep_recent=len(keep_recent),
        keep_monthly=len(keep_monthly),
        extras=len(delete_extras),
    )

    domain_rows = top_removals_by_domain(delete_extras, keep_monthly, topn=10)
    if domain_rows:
        show_domain_table(lang, domain_rows)

    if delete_extras:
        show_preview_delete(lang, delete_extras, limit=12)

    step_rule(lang, 6, tr(lang, "export_plan"))
    plan_folder = os.path.join(os.getcwd(), f"PingCastlePlan-{now_stamp()}")
    paths = export_all_plan(plan_folder, reports_all, keep_recent, keep_monthly, delete_extras)
    ok(tr(lang, "plan_exported"))
    console.print(f"{tr(lang, 'plan_folder')}: [bold]{plan_folder}[/bold]")
    console.print(f"  ALL:            {paths['ALL']}")
    console.print(f"  KEEP recent:     {paths['KEEP_Recent']}")
    console.print(f"  KEEP monthly:    {paths['KEEP_Monthly']}")
    console.print(f"  DELETE extras:   {paths['DELETE_Extras']}")
    log_write(logfile, f"[plan] folder={plan_folder}")

    if not delete_extras:
        ok(tr(lang, "no_extras"))
        conn.rollback()
        console.print(f"{tr(lang, 'logfile')}: {logfile}")
        return 0

    default_archive = safe_default_archive_dir()
    console.print(f"\n[dim]{tr(lang, 'safe_default_archive')}: {default_archive}[/dim]\n")

    # Default prompt must show archive/delete
    action = Prompt.ask(tr(lang, "action_old"), default="archive/delete").strip().lower()
    if action not in ("archive", "delete"):
        action = "archive"

    archive_dir = ""
    archive_fmt = "jsonl"
    make_zip = True

    if action == "archive":
        archive_dir = Prompt.ask(tr(lang, "archive_dir"), default=default_archive).strip()
        while not validate_writable_dir(archive_dir):
            warn(f"{tr(lang, 'archive_dir')} not writable. Please choose another path.")
            archive_dir = Prompt.ask(tr(lang, "archive_dir"), default=default_archive).strip()

        archive_fmt = Prompt.ask(tr(lang, "archive_fmt"), default="jsonl").strip().lower()
        if archive_fmt not in ("jsonl", "csv"):
            archive_fmt = "jsonl"

        make_zip = Confirm.ask(tr(lang, "zip_q"), default=True)

    step_rule(lang, 7, tr(lang, "dryrun_prep"))

    ids_to_delete = [r.id for r in delete_extras]
    create_delete_ids_temp(cur, ids_to_delete)
    counts = dryrun_counts(cur, reports, deps)
    show_dryrun_table(lang, counts)
    ok(f"{tr(lang, 'dryrun_ready')}: {len(ids_to_delete)}")
    log_write(logfile, f"[dryrun] delete_reports={len(ids_to_delete)}")

    if not Confirm.ask(tr(lang, "apply"), default=False):
        ok(tr(lang, "dryrun_only"))
        conn.rollback()
        log_write(logfile, "Dry-run only. ROLLBACK.")
        console.print(f"{tr(lang, 'logfile')}: {logfile}")
        return 0

    try:
        # Archive first
        if action == "archive":
            step_rule(lang, 8, tr(lang, "archiving"))
            out_folder = os.path.join(archive_dir, f"PingCastleArchive-{now_stamp()}")
            ensure_dir(out_folder)

            def do_archive():
                if archive_fmt == "jsonl":
                    return export_archive_jsonl(cur, reports, ids_to_delete, out_folder)
                return export_archive_csv(cur, reports, ids_to_delete, out_folder)

            archive_file = progress_run("Exporting archive", do_archive)
            ok(f"{tr(lang, 'archive_created')}: {archive_file}")

            if make_zip:
                zip_path = out_folder + ".zip"
                zip_folder(out_folder, zip_path)
                ok(f"{tr(lang, 'zip_created')}: {zip_path}")
                log_write(logfile, f"[archive] zip={zip_path}")
            else:
                log_write(logfile, f"[archive] folder={out_folder}")

        # Final confirmation
        step_rule(lang, 9, tr(lang, "deleting"))
        if not Confirm.ask(tr(lang, "final_confirm"), default=False):
            ok(tr(lang, "cancelled"))
            conn.rollback()
            log_write(logfile, "Cancelled before delete. ROLLBACK.")
            console.print(f"{tr(lang, 'logfile')}: {logfile}")
            return 0

        # Ensure temp table exists and pointers are fixed
        create_delete_ids_temp(cur, ids_to_delete)
        update_domains_first_last(cur, reports, dom_schema, dom_table)

        def do_delete():
            return delete_children_then_reports(cur, reports, deps)

        with Progress(SpinnerColumn(), TextColumn("{task.description}"), TimeElapsedColumn(), console=console) as p:
            t = p.add_task("Deleting...", total=None)
            deleted = do_delete()
            p.update(t, completed=1)

        conn.commit()
        ok(tr(lang, "done"))
        log_write(logfile, f"[delete] committed. deleted={deleted}")

    except Exception as e:
        conn.rollback()
        err(f"{tr(lang, 'err')}: {e}")
        log_write(logfile, "[EXCEPTION]")
        log_write(logfile, traceback.format_exc())
        console.print(f"{tr(lang, 'logfile')}: {logfile}")
        return 3

    console.print(f"{tr(lang, 'logfile')}: {logfile}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
