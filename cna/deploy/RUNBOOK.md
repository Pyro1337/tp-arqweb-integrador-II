# Runbook de despliegue — Bloque II

> Antes de actualizar una instalación existente: revisar
> [mejoras y aceptación pendiente](../docs/MEJORAS_Y_VALIDACION_PENDIENTE.md).
> Streams NATS incompatibles fallan cerrado y las colas legacy de Redis se deben
> drenar con la versión vieja antes del corte; no se migran/borran automáticamente.
> Los siguientes pasos son para laboratorio autorizado, no para el VPS de producción.

## 0. Variables que vas a necesitar a mano
- IP de VM-1 (gitea): la que ya tenés (`ip addr` en VM-1).
- IP de VM-2 (la clonás en el paso 1): anotala apenas la tengas, se usa en todos los pasos siguientes.

## 1. Clonar VM-2 (B1)

En VirtualBox/VMware: clic derecho sobre tu VM -> **Clonar** -> clon enlazado -> nombre `cna-node1`. Arrancala y renombrá el hostname:

```bash
sudo hostnamectl set-hostname cna-node1
ip addr show enp0s3 | grep inet
```

## 2. Instalar Docker en VM-2

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
# cerrar sesion SSH y volver a entrar para que tome el grupo docker
```

## 3. UFW en VM-2 (solo acepta desde VM-1)

Reemplazá `IP_DE_VM1` por la IP real:

```bash
sudo ufw allow from IP_DE_VM1 to any port 22,4222,7700,5432,6379,9000,9001,8001,8002,8003,8004,8005 proto tcp
sudo ufw enable
sudo ufw status verbose
```

## 4. Copiar el código a VM-2

Desde tu PC Windows, con la carpeta `cna/` ya en tu repo:

```bash
scp -r "C:\Users\PYRO1337\Desktop\TP Arquitectura WEB\tp-arqweb-integrador-II\cna" arqweb2026@IP_DE_VM2:~/cna
```

(usá el usuario real de VM-2, el mismo que configuraste al clonar)

## 5. Crear el usuario de servicio y el token en Gitea (en VM-1, vía navegador o API)

1. Entrá a `https://gitea.local/-/admin/users/new` (o la ruta de admin de tu version) y creá un usuario `svc-cna` con permisos de solo lectura sobre los repos que uses.
2. Logueado como `svc-cna`, andá a **Settings -> Applications** y generá un token con scope de lectura de repos.
3. Copiá el token, lo vas a necesitar en el `.env`.

## 6. Armar el `.env` en VM-2

```bash
cd ~/cna/deploy
cp .env.example .env
nano .env
```

Completar:
```
GITEA_URL=http://IP_DE_VM1:3000
GITEA_TOKEN=<el token del paso 5>
WEBHOOK_SECRET=<inventar algo largo, ej con: openssl rand -hex 16>
VM2_IP=<la IP de esta misma VM-2>
```

## 7. Levantar todo con un comando (B5 - CA-7)

```bash
cd ~/cna/deploy
docker compose --env-file .env up -d --build
docker compose ps
```

Esperá que todos digan `healthy`. Evidencia **F6-E16**.

## 8. Configurar el Webhook en Gitea (B4)

En el repo de prueba (o a nivel organización): **Settings -> Webhooks -> Add Webhook -> Gitea**:
- URL: `http://IP_DE_VM2:8001/webhook/<el WEBHOOK_SECRET que pusiste en el .env>`
- Content type: `application/json`
- Trigger: Push events, Issue events

Guardalo y probá "Test Delivery". Evidencia **F6-E11**.

## 9. Gateway en VM-1 (B5)

```bash
# Copiar el archivo real y editar los upstreams/hostname/rutas TLS para el laboratorio.
sudo cp nginx-api.equipo01.arqweb.test.conf /etc/nginx/sites-available/api.equipo01.arqweb.test
sudoedit /etc/nginx/sites-available/api.equipo01.arqweb.test
sudo ln -s /etc/nginx/sites-available/api.equipo01.arqweb.test /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

### Certificado con doble SAN
Si ya tenías un cert autofirmado para `gitea.local`, regeneralo agregando `api.gitea.local` al SAN (openssl o mkcert, lo que hayan usado en el TP1), y apuntalo en `/etc/ssl/gitea/`.

Agregar al `/etc/hosts` del anfitrión Windows (`C:\Windows\System32\drivers\etc\hosts`, como administrador):
```
IP_DE_VM1   api.gitea.local
```

## 10. Probar de punta a punta

```bash
# push de prueba a un repo de Gitea, despues:
curl -k "https://api.gitea.local/v2/search?q=algo-que-pusheaste"

# auditoria
curl -k "https://api.gitea.local/v2/audit"

# exportacion
curl -k -X POST "https://api.gitea.local/v2/exports" -H "Content-Type: application/json" \
  -d '{"repo":"usuario/repo","format":"csv","scope":"activity"}'
curl -k "https://api.gitea.local/v2/exports/<job_id-que-te-devolvio>"

# media
curl -k -X POST "https://api.gitea.local/v2/media" -F "file=@imagen.png" -F "repo=usuario/repo" -F "ref_type=issue"
```

## 11. Evidencias a capturar ahora
- F6-E1: `docker compose ps` con todo healthy
- F6-E2: `sudo ufw status verbose` en VM-2
- F6-E4: resultado de `/v2/search` antes y después de un push
- F6-E5: mandar el mismo evento dos veces (reenviar webhook desde Gitea) y mostrar que audit-svc solo guarda un registro
- F6-E6 / F6-E7: ciclo completo de export + objeto visible en la consola de MinIO (`http://IP_DE_VM2:9001`)
- F6-E8: subida y descarga de media
- F6-E9/E10: `docker images` y `docker inspect` (usuario, límites, healthcheck)
- F6-E11/E12/E13: webhook configurado, evento visto en NATS (`docker exec -it <nats_container> nats stream ls`), forzar una DLQ
- F6-E14/E15/E16: `nginx -t`, un 429 pegándole rápido al endpoint, y el deploy en un comando
