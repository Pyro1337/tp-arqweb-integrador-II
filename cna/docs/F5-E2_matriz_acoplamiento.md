# F5-E2 — Matriz de acoplamiento módulo x tabla (Gitea)

Para esto partimos de la salida de `sqlite3 gitea.db ".tables"` (la evidencia F5-E1) y fuimos agrupando las tablas según a qué parte de Gitea le corresponden. Son casi 140 tablas en total, así que no tenía sentido analizarlas una por una, mejor las juntamos por bloque funcional.

## M1 - Repositorio / contenido

Este es el módulo que le interesa a S1 (search-svc).

repository, repo_unit, repo_topic, topic, repo_redirect, repo_transfer, repo_license, repo_archiver, repo_indexer_status, branch, protected_branch, protected_tag, commit_status, commit_status_index, commit_status_summary, mirror, push_mirror, lfs_lock, lfs_meta_object, language_stat, star, watch, collaboration, deploy_key, renamed_branch.

## M2 - Issues y Pull Requests

También le aporta a S1, porque la búsqueda tiene que encontrar tanto código como issues, no solo una cosa.

issue, issue_assignees, issue_label, issue_pin, issue_user, issue_watch, issue_content_history, issue_dependency, issue_index, comment, label, milestone, project, project_board, project_issue, pull_request, pull_auto_merge, review, review_state, reaction, tracked_time, stopwatch.

## M3 - Actividad / auditoría / automatización

De acá sale S2 (audit-svc). Entra todo lo de Actions de Gitea (que de hecho no vamos a usar nosotros, pero están ahí) y los webhooks.

action, action_artifact, action_run, action_run_attempt, action_run_attempt_job_id_index, action_run_index, action_run_job, action_run_job_summary, action_runner, action_runner_token, action_schedule, action_schedule_spec, action_scoped_workflow_source, action_task, action_task_output, action_task_step, action_tasks_version, action_variable, notice, notification, hook_task, webhook, task.

## M4 - Exportación / adjuntos / paquetes

De acá sacamos S3 (export-svc) y, si nos da el tiempo, también S4 (media-svc).

attachment, upload, release, package, package_blob, package_blob_upload, package_cleanup_rule, package_file, package_property, package_version, badge.

## M5 - Usuarios, permisos y seguridad

Este no se toca, se queda en el monolito tal cual está. Es el que se encarga de login, tokens y quién puede entrar a qué repo.

user, user_badge, user_blocking, user_open_id, user_redirect, user_setting, org_user, team, team_invite, team_repo, team_unit, team_user, access, access_token, auth_token, login_source, external_login_user, gpg_key, gpg_key_import, public_key, two_factor, webauthn_credential, session, email_address, email_hash, oauth2_application, oauth2_authorization_code, oauth2_grant, secret, follow.

## M6 - Configuración interna

app_state, system_setting, version, dbfs_data, dbfs_meta. Tampoco se toca, es config interna de la instancia.

## Las tablas que generan lío (acoplamiento)

Mirando el esquema nos dimos cuenta que hay un par de tablas que están por todos lados y que justamente por eso no se puede ir directo contra la base:

- `repository`: está referenciada desde casi cualquier módulo (issue, action, attachment, release, webhook, lo que sea) por `repo_id`. Es la tabla con más dependencias de lejos.
- `user`: aparece como `owner_id`, `poster_id` o `uid` en issues, comentarios, actions, webhooks. Concentra toda la identidad y los permisos.
- `access` / `access_token`: define si alguien puede leer o escribir un repo. Ningún servicio externo puede saltearse esto leyendo la tabla directo, tiene que respetar lo que diga la API.
- `webhook` / `hook_task`: justo es el mecanismo que elegimos para integrarnos (lo justificamos en el ADR-006), así que por diseño los microservicios no tocan esta tabla, reciben el webhook ya disparado por Gitea vía HTTP.

Por todo esto, ningún microservicio se va a conectar directo a `gitea.db`. Lo que es consulta de contenido pasa por la API REST de Gitea con un token de servicio, y lo que es un cambio se entera por el camino Webhook -> event-bridge -> broker.
