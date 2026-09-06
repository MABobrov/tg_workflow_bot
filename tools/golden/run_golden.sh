#!/usr/bin/env bash
# Прогон golden-снимка денежных витрин.
#
# Смысл: до правки и после правки — один и тот же прогон, побайтовое сравнение.
# Расхождения обязаны быть РОВНО там, где правка задумана, и нигде больше.
#
#   ./run_golden.sh              сверить текущий код прода с эталоном
#   ./run_golden.sh --baseline   переснять эталон (после осознанной правки!)
#   APP=/tmp/my_app ./run_golden.sh   сверить ДРУГОЕ дерево кода (ветка, патч)
#
# Что трогает: НИЧЕГО боевого. Код монтируется только на чтение, БД — замороженный
# снимок /root/_golden/db/golden.sqlite3, и даже он открывается копией в /tmp
# внутри контейнера. Сети у контейнера нет вовсе (--network none).
#
# Эталон переснимать ТОЛЬКО осознанно: он и есть память о том, как код считал деньги.
set -uo pipefail

IMAGE="${IMAGE:-tg_workflow_bot-workflow-bot}"
APP="${APP:-/root/tg_workflow_bot/app}"
# Сам инструмент — в git (tools/golden рядом с этим файлом).
TOOLS="${TOOLS:-$(cd "$(dirname "$0")" && pwd)}"
# 🔴 Снимок боевой БД и результаты — ВНЕ репозитория и вне GitHub НАМЕРЕННО:
# в golden.sqlite3 лежат настоящие суммы, счета и люди.
DATA="${DATA:-/root/_golden}"
OUT="${OUT:-$DATA/out}"

mkdir -p "$OUT"
echo "образ: $IMAGE | код: $APP | инструмент: $TOOLS | снимок БД: $DATA/db/golden.sqlite3"

timeout 1800 docker run --rm --network none \
  -v "$DATA/db":/golden:ro \
  -v "$OUT":/out \
  -v "$APP":/app/app:ro \
  -v "$TOOLS":/tools:ro \
  -v "$TOOLS/golden_snapshot.py":/app/golden_snapshot.py:ro \
  "$IMAGE" python /app/golden_snapshot.py "$@"
RC=$?

echo
if [ "$RC" = 0 ]; then
  echo "результат: расхождений нет (или записан эталон)"
elif [ "$RC" = 1 ]; then
  echo "результат: ЕСТЬ РАСХОЖДЕНИЯ — разобрать каждое, полный текст в $OUT/dump.txt"
elif [ "$RC" = 2 ]; then
  echo "результат: эталона нет, сначала ./run_golden.sh --baseline"
elif [ "$RC" = 124 ]; then
  echo "результат: ТАЙМАУТ 30 мин — контейнер убит"
else
  echo "результат: прогон упал (код $RC)"
fi
echo "непокрытое: $OUT/skipped.json | нестабильное: $OUT/unstable.json | ошибки: $OUT/errors.json"
exit $RC
