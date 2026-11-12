# Versionado de contratos

## API REST

Todas las rutas de los microservicios van con el prefijo `/v2/` (por ejemplo `/v2/search`, `/v2/audit`). Arrancamos en 2 porque conceptualmente la v1 es la API nativa de Gitea, que seguimos usando por detrás para leer datos.

Si en algún momento necesitamos un cambio que rompa compatibilidad (por ejemplo cambiar cómo viene la respuesta de `/v2/search`), la idea es agregar un server block nuevo para `/v3/` en el gateway, y que convivan los dos hasta que todos los consumidores terminen de migrar. Decidimos versionar por path y no por header porque es mucho más fácil de probar con curl y de ver en los logs de nginx qué versión está pegando cada uno.

## Eventos

Cada evento lleva un campo `version` adentro del sobre (está en `events.asyncapi.yaml`), separado del nombre del canal. Por ejemplo, el canal `gitea.push` manda eventos con `version: v1`. Si en algún momento cambia la forma del payload sin que cambie el significado del evento, subimos la versión pero seguimos usando el mismo canal, así los consumidores viejos pueden seguir leyendo mientras se actualizan (el campo version les sirve para saber si entienden ese payload o no). Si el cambio es grande, directo armamos un canal nuevo en vez de pisar el viejo, por ejemplo `gitea.push.v2`.
