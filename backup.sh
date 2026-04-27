#!/bin/bash
# Backup precios.db to Google Drive via rclone
# Cron: 0 * * * * /home/jhonnyvb8/ricardowatcher/backup.sh >> /home/jhonnyvb8/ricardowatcher/backup.log 2>&1

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DB="$SCRIPT_DIR/precios.db"
TMP="$SCRIPT_DIR/precios_backup.db"
REMOTE="gdrive:ricardowatcher-backups"
KEEP=24  # backups to keep (24 × 1h = 1 day of history)

if [ ! -f "$DB" ]; then
    echo "[$(date)] ERROR: $DB not found"
    exit 1
fi

# Safe SQLite backup (works even while the bot is writing)
sqlite3 "$DB" ".backup '$TMP'"
if [ $? -ne 0 ]; then
    echo "[$(date)] ERROR: sqlite3 backup failed"
    exit 1
fi

# Upload with timestamp
FILENAME="precios_$(date +%Y%m%d_%H%M).db"
~/.local/bin/rclone copy "$TMP" "$REMOTE" --drive-use-trash=false 2>&1
~/.local/bin/rclone moveto "$REMOTE/precios_backup.db" "$REMOTE/$FILENAME" 2>/dev/null || true
~/.local/bin/rclone copy "$TMP" "$REMOTE/precios_backup.db" 2>&1

rm -f "$TMP"

# Keep only last $KEEP backups (delete oldest)
~/.local/bin/rclone delete "$REMOTE" --min-age 0 --filter "- precios_backup.db" \
    --filter "+ precios_*.db" --filter "- *" 2>/dev/null || true
COUNT=$(~/.local/bin/rclone lsf "$REMOTE" --filter "+ precios_2*.db" --filter "- *" 2>/dev/null | wc -l)
if [ "$COUNT" -gt "$KEEP" ]; then
    EXCESS=$((COUNT - KEEP))
    ~/.local/bin/rclone lsf "$REMOTE" --filter "+ precios_2*.db" --filter "- *" 2>/dev/null \
        | sort | head -n "$EXCESS" \
        | while read f; do ~/.local/bin/rclone delete "$REMOTE/$f" 2>/dev/null; done
fi

echo "[$(date)] Backup OK → $REMOTE/$FILENAME"
