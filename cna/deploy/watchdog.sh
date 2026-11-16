#!/bin/bash
# Watchdog de recuperacion: el daemon de Docker en este entorno no aplica
# la politica de reinicio nativa (restart: unless-stopped) tras un SIGKILL,
# asi que este script hace ese trabajo desde afuera, revisando cada contenedor
# del proyecto y reiniciando el que este Exited.
cd /home/arqweb2026/cna/deploy || exit 1

for name in $(docker compose ps -a --format '{{.Name}}'); do
  status=$(docker inspect "$name" --format '{{.State.Status}}' 2>/dev/null)
  if [ "$status" = "exited" ]; then
    echo "$(date -u +%FT%TZ) watchdog: $name estaba exited, reiniciando" >> /home/arqweb2026/watchdog.log
    docker start "$name" >> /home/arqweb2026/watchdog.log 2>&1
  fi
done
