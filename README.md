# CloudLambda - Microservicios Serverless (AWS)

Repositorio de **microservicios serverless** del sistema académico **CloudEvaluacion**, implementados con **AWS Lambda (Node.js 20)**. Estos componentes actúan como **consumidores** de eventos publicados en **AWS SNS**, como **ejecutores** de la lógica de negocio y como **único punto de acceso a la base de datos** (**Supabase / PostgreSQL**).

---

## Descripción

El sistema **CloudEvaluacion** sigue el patrón:

```
Page  →  API Gateway  →  BFF  →  SNS  →  SQS  →  Lambda  →  Supabase
 (SPA)       (proxy)    (prod)  (topic)  (queue) (consumer)   (DB)
```

Este repositorio cubre el tramo **SQS → Lambda → Supabase** (más las llamadas síncronas desde el BFF para lecturas). Las Lambdas:

- **Reciben** comandos de escritura desde el BFF a través de SNS → SQS (operaciones asíncronas).
- **Responden** a invocaciones directas desde el BFF para operaciones de lectura (operaciones síncronas).
- **Publican** eventos de notificación de vuelta al topic SNS (patrón *fan-out*).
- **Persisten** los cambios en **Supabase (PostgreSQL)**.
- **Notifican** a un servicio externo (Linode) y envían correos cuando corresponda.

---

## Arquitectura

```
+--------------+    HTTPS     +--------------+    HTTP     +------------------+
|              |------------->|              |------------>|                  |
|  React SPA   |  Bearer JWT  | AWS API      |  + header   |  EC2             |
|  (Vercel)    |              | Gateway      |  X-Secret-  |  +------------+  |
|              |<-------------| (HTTP API)   |  Gateway    |  | Spring Boot|  |
|              |              |              |             |  | BFF (Docker)|  |
+--------------+              +--------------+             |  +-----+------+  |
       |                                                     +--------+--------+
       |                                                              |
       |  Authorization Code                                          |
       v                                                              |
+--------------+                                                      |
| AWS Cognito  |                                                      |
| (User Pool)  |                                                      |
+--------------+                                                      |
                                                                      |
                                        +-----------------------------+
                                        |
                                        | (Asíncrono)
                                        v
                               +------------------+
                               |    AWS SNS       |
                               |  (Topic:         |
                               | CloudStudyEvents)|
                               +--------+---------+
                                        |
                                        | Filter Policy
                                        v
                               +------------------+
                               |    AWS SQS       |
                               |  DbWriteQueue    |
                               |  NotificationQueue|
                               +--------+---------+
                                        |
                                        | reintentos
                                        |
                                        v
                               +------------------+                            +------------------+
                               |   AWS Lambda     |                            |   Notification   |
                               |  (Consumer)      |--------------------------->|       API        |
                               +--------+---------+                            +--------+---------+
                                        |
                                        |
                                        v
     +--------------------------------------------------+
     |              Supabase (PostgreSQL)               |
     +--------------------------------------------------+
```

---

## Patrón de mensajería (EP3 · versión AWS)

El sistema implementa mensajería **desacoplada y orientada a eventos** utilizando servicios gestionados de AWS. Cada concepto del patrón de mensajería se materializa con un servicio administrado:

| Concepto de mensajería | Implementación AWS | Estado en este repo |
|---|---|---|
| **Exchange** | **AWS SNS Topic** (`CloudStudyEvents`) | ✅ Publicación desde el BFF y desde las Lambdas |
| **Queue** | **AWS SQS** (`DbWriteQueue`, `NotificationQueue`) | ✅ Reciben los mensajes desde SNS |
| **Binding** | Suscripción SNS → SQS con **Filter Policy** | ✅ Enrutamiento por atributo (`action` / `type`) |
| **Producer** | **BFF** (Java) y **Lambdas** (JS) | ✅ `AwsSnsClient` en el BFF; helper `publishEvent` en las Lambdas |
| **Consumer** | **AWS Lambda** con *event source mapping* a SQS | ✅ Todos los `index.js` detectan `event.Records` |

### Topología completa (estado actual)

```
                                       ┌──────────────────────────┐
                                       │        AWS SNS           │
                                       │  CloudStudyEvents        │
                                       └────────────┬─────────────┘
                                                    │
                              ┌─────────────────────┴─────────────────────┐
                              │                                           │
                    Filter: action ∈ {…}                    Filter: type ∈ {…}
                              │                                           │
                              v                                           v
                    ┌─────────────────┐                       ┌─────────────────────┐
                    │  SQS DbWrite    │                       │  SQS Notification   │
                    │  Queue          │                       │  Queue              │
                    └────────┬────────┘                       └──────────┬──────────┘
                             │                                           │
              ┌──────────────┼──────────────┐                            │
              │              │              │                            │
              v              v              v                            v
    ┌─────────────────┐ ┌─────────────┐ ┌──────────────┐    ┌─────────────────────┐
    │ lambda-course   │ │ lambda-     │ │ lambda-task  │    │ lambda-notification │
    │ -service        │ │ enrollment- │ │ -service     │    │ -worker             │
    │                 │ │ service     │ │              │    │                     │
    └────────┬────────┘ └──────┬──────┘ └──────┬───────┘    └──────────┬──────────┘
             │                 │               │                       │
             │                 │               │                       │
             └─────────────────┴───────┬───────┘                       │
                                       │                               │
                                       │ publica eventos               │
                                       │ (type=*)                       │ envía
                                       v                               v
                              ┌─────────────────┐              ┌──────────────┐
                              │   AWS SNS       │              │ Notification │
                              │ (feedback)      │              │     API      │
                              └─────────────────┘              └──────────────┘
                                       │
                                       │ (fan-out)
                                       v
                              ┌─────────────────┐
                              │ SQS Notif. Queue│
                              └─────────────────┘
                                       │
                                       v
                         ┌──────────────────────────┐
                         │ lambda-notification-worker│
                         │  (email si es REPORT)     │
                         └──────────────────────────┘
```

### Enrutamiento por Filter Policy

Los suscriptores SNS aplican **Filter Policies** basadas en atributos de mensaje para recibir solo los eventos que les competen:

| Cola | Atributo de filtro | Valores |
|---|---|---|
| `DbWriteQueue` | `action` | `ADMIN_CREATE_COURSE`, `ADMIN_ASSIGN_TEACHER`, `ADMIN_DELETE_COURSE`, `ADMIN_ENROLL_STUDENT`, `TEACHER_ENROLL_STUDENT`, `STUDENT_ENROLL`, `TEACHER_CREATE_TASK`, `GENERATE_REPORT` |
| `NotificationQueue` | `type` | `COURSE_CREATED`, `TEACHER_ASSIGNED`, `COURSE_DELETED`, `STUDENT_ENROLLED`, `TASK_CREATED`, `REPORT_GENERATED` |

> **Nota:** el **BFF publica comandos** con atributo `action=*` (los consume `DbWriteQueue`); las **Lambdas publican eventos** con atributo `type=*` (los consume `NotificationQueue`). Esta separación permite que la cadena de notificaciones (SNS → SQS → Notification Worker) sea independiente de la cadena de procesamiento de escrituras.

---

## Stack tecnológico

| Componente | Versión / Herramienta |
|---|---|
| Node.js | 20+ |
| AWS SDK for JavaScript v3 | `@aws-sdk/client-sns` |
| Driver PostgreSQL | `pg` 8.x |
| Runtime | AWS Lambda (Node.js 20.x) |
| CI/CD | GitHub Actions (matrix strategy) |

### Dependencias comunes

```json
{
  "dependencies": {
    "@aws-sdk/client-sns": "^3.540.0",
    "pg": "^8.11.3"
  }
}
```

`lambda-notification-worker` no declara dependencias externas (usa `fetch` nativo de Node 20).

---

## Estructura del repositorio

```
CloudLambda/
├── .github/
│   └── workflows/
│       └── deploy.yml             # CI/CD con matrix strategy
├── lambda-course-service/
│   ├── index.js                   # CRUD de cursos + consultas
│   ├── package.json
│   └── package-lock.json
├── lambda-enrollment-service/
│   ├── index.js                   # Inscripciones + consultas por curso
│   ├── package.json
│   └── package-lock.json
├── lambda-task-service/
│   ├── index.js                   # Tareas + consultas por curso/alumno
│   ├── package.json
│   └── package-lock.json
├── lambda-report-generator/
│   ├── index.js                   # Generación de reportes y publicación a SNS
│   ├── package.json
│   └── package-lock.json
└── lambda-notification-worker/
    ├── index.js                   # Consume SQS → Notice API + email
    └── package.json
```

> No existe `template.yaml` en el repositorio: la topología AWS (topic, colas, filtros, mappings) se administra manualmente. Es un ítem pendiente para cerrar la brecha de IaC de la rúbrica.

---

## Responsabilidades por Lambda

### `lambda-course-service`

**Dominio:** cursos.

| Acción | Tipo | Descripción |
|---|---|---|
| `ADMIN_LIST_COURSES` | Sync | Lista todos los cursos |
| `ADMIN_GET_COURSE` | Sync | Obtiene un curso por ID |
| `ADMIN_CREATE_COURSE` | Async (SQS) | Crea un curso y publica `COURSE_CREATED` |
| `ADMIN_ASSIGN_TEACHER` | Async (SQS) | Asigna docente y publica `TEACHER_ASSIGNED` |
| `ADMIN_DELETE_COURSE` | Async (SQS) | Elimina curso y publica `COURSE_DELETED` |
| `TEACHER_GET_COURSES` | Sync | Lista cursos asignados al docente |
| `STUDENT_GET_AVAILABLE` | Sync | Lista cursos donde el alumno no está inscrito |

### `lambda-enrollment-service`

**Dominio:** inscripciones.

| Acción | Tipo | Descripción |
|---|---|---|
| `ADMIN_ENROLL_STUDENT` | Async (SQS) | Inscribe a un alumno (sin validar ownership) |
| `TEACHER_ENROLL_STUDENT` | Async (SQS) | Inscribe validando que el docente sea dueño del curso |
| `STUDENT_ENROLL` | Async (SQS) | Auto-inscripción del alumno |
| `ADMIN_GET_STUDENTS` | Sync | Lista alumnos de un curso |
| `TEACHER_GET_STUDENTS` | Sync | Lista alumnos validando ownership |
| `STUDENT_GET_COURSES` | Sync | Lista cursos inscritos del alumno |

Al crear una inscripción se publica `STUDENT_ENROLLED`.

### `lambda-task-service`

**Dominio:** tareas.

| Acción | Tipo | Descripción |
|---|---|---|
| `TEACHER_CREATE_TASK` | Async (SQS) | Crea una tarea validando ownership del docente |
| `TEACHER_GET_TASKS` | Sync | Lista tareas del curso (validando ownership) |
| `STUDENT_GET_COURSE_TASKS` | Sync | Lista tareas si el alumno está inscrito |
| `STUDENT_GET_TASKS` | Sync | Lista todas las tareas del alumno (join con enrollments) |

Al crear una tarea se publica `TASK_CREATED`.

### `lambda-report-generator`

**Dominio:** analítica.

| Acción | Tipo | Descripción |
|---|---|---|
| `GENERATE_REPORT` | Async (SQS) | Agrega cursos + inscripciones + tareas, persiste en `reports` y publica `REPORT_GENERATED` con `requestedBy` (email del admin) y los datos crudos para el envío de correo |

### `lambda-notification-worker`

**Dominio:** notificaciones.

| Trigger | Procesamiento |
|---|---|
| SQS (`NotificationQueue`) | Envía el texto a la **Linode Notice API** |
| SQS (`NotificationQueue`) si `type === 'REPORT_GENERATED'` | Envía correo HTML + texto al `requestedBy` con el resumen del reporte |

> Es el único componente que se comunica con servicios externos (Linode). Mantener esa responsabilidad aislada garantiza que un fallo del proveedor externo no bloquee la lógica de negocio (que ya está persistida).

---

## Contrato de eventos

### Publicado por el BFF (comandos)

Formato del cuerpo:

```json
{
  "action": "ADMIN_CREATE_COURSE",
  "data": {
    "name": "Desarrollo Cloud Native",
    "description": "DSY1107",
    "teacherId": "profesor@duoc.cl"
  }
}
```

Atributo SNS: `action=ADMIN_CREATE_COURSE` → enrutado a `DbWriteQueue` vía Filter Policy.

### Publicado por las Lambdas (eventos de notificación)

Formato del cuerpo:

```json
{
  "type": "COURSE_CREATED",
  "message": "New course created: Desarrollo Cloud Native",
  "data": { "...": "..." }
}
```

Atributo SNS: `type=COURSE_CREATED` → enrutado a `NotificationQueue` vía Filter Policy.

### Eventos soportados

| `type` publicado | Origen | Consumidor |
|---|---|---|
| `COURSE_CREATED` | `lambda-course-service` | `lambda-notification-worker` |
| `TEACHER_ASSIGNED` | `lambda-course-service` | `lambda-notification-worker` |
| `COURSE_DELETED` | `lambda-course-service` | `lambda-notification-worker` |
| `STUDENT_ENROLLED` | `lambda-enrollment-service` | `lambda-notification-worker` |
| `TASK_CREATED` | `lambda-task-service` | `lambda-notification-worker` |
| `REPORT_GENERATED` | `lambda-report-generator` | `lambda-notification-worker` (envía correo) |

### Estructura del `record` de SQS

Los handlers de Lambda reciben el evento de SQS con el envelope de SNS dentro del `record.body`:

```json
{
  "Records": [
    {
      "messageId": "...",
      "body": "{\"Type\":\"Notification\",\"Message\":\"{...}\",\"MessageAttributes\":{\"type\":{\"Type\":\"String\",\"Value\":\"COURSE_CREATED\"}}}",
      "receiptHandle": "...",
      "...": "..."
    }
  ]
}
```

Los handlers deserializan `record.body` → `snsEnvelope.Message` → `payload.action` / `payload.data`.

---

## Separación de lógica

Cada `index.js` sigue la misma estructura para mantener la separación de responsabilidades exigida por la rúbrica:

```
┌────────────────────────────────────────────────────────┐
│ 1. Configuración (top-level)                           │
│    - Pool de PostgreSQL (pg.Pool)                      │
│    - Cliente SNS (SNSClient)                           │
│    - Helper publishEvent(...)                          │
├────────────────────────────────────────────────────────┤
│ 2. Router asíncrono/síncrono (exports.handler)         │
│    - if (event.Records) → procesamiento SQS            │
│    - else → invocación directa desde el BFF            │
├────────────────────────────────────────────────────────┤
│ 3. Lógica de negocio (handleAction)                    │
│    - switch (action) con las operaciones de dominio    │
│    - Sin mezclar configuración de red ni de mensajería │
└────────────────────────────────────────────────────────┘
```

### Beneficios

- **Configuración aislada**: el pool de PostgreSQL se inicializa una vez (fuera del handler) y se reutiliza en invocaciones *warm*.
- **Router explícito**: el handler detecta el origen del evento y delega.
- **Lógica pura**: `handleAction` recibe `(action, data)` y devuelve una respuesta; no conoce detalles de SQS, SNS ni del runtime de Lambda.
- **Reutilización**: el helper `publishEvent(type, message, data)` se comparte en los servicios que emiten eventos.

---

## Requisitos previos

- **Node.js 20+** (para desarrollo local).
- **AWS CLI v2** configurado con credenciales de una cuenta con permisos para:
  - Gestionar SNS, SQS, Lambda y CloudWatch Logs.
  - (Cuando se añada IaC) Crear/actualizar stacks de CloudFormation.
- **Supabase** (PostgreSQL) con las tablas `courses`, `enrollments`, `tasks` y `reports`.
- **Notification API** (Linode) accesible con `NOTIFICATION_API_KEY`.

---

## Configuración y variables de entorno

Cada Lambda recibe las variables de entorno definidas en la consola de AWS (no versionadas en este repo). Las variables comunes son:

| Variable | Descripción | Ejemplo |
|---|---|---|
| `DATABASE_URL` | Cadena de conexión a PostgreSQL (Supabase) | `postgresql://postgres.<ref>:<pwd>@aws-0-us-west-2.pooler.supabase.com:5432/postgres` |
| `SNS_TOPIC_ARN` | ARN del topic `CloudStudyEvents` | `arn:aws:sns:us-east-1:123456789012:CloudStudyEvents` |
| `AWS_REGION` | Región de AWS | `us-east-1` |
| `NOTIFICATION_API_KEY` | API Key del servicio externo (Linode) — solo `lambda-notification-worker` | `xxxxxxxx` |

### Buenas prácticas

- **Nunca** comitear credenciales: usar **AWS Secrets Manager** o **SSM Parameter Store** para `DATABASE_URL` y `NOTIFICATION_API_KEY`.
- Configurar las variables por entorno (`dev`, `prod`) desde la consola o desde `sam deploy` (cuando exista `template.yaml`).

---

## Ejecución local

Para probar una Lambda sin desplegarla:

```bash
cd lambda-course-service
npm install

# Simular invocación directa (lectura)
DATABASE_URL=postgresql://... \
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:...:CloudStudyEvents \
AWS_REGION=us-east-1 \
node -e "
const { handler } = require('./index.js');
handler({ action: 'ADMIN_LIST_COURSES' })
  .then(r => console.log(r));
"

# Simular procesamiento SQS (escritura)
DATABASE_URL=postgresql://... \
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:...:CloudStudyEvents \
AWS_REGION=us-east-1 \
node -e "
const { handler } = require('./index.js');
handler({
  Records: [{
    body: JSON.stringify({
      Message: JSON.stringify({
        action: 'ADMIN_CREATE_COURSE',
        data: { name: 'Test', description: 'Demo', teacherId: 'prof@duoc.cl' }
      })
    })
  }]
}).then(r => console.log(r));
"
```

Requiere credenciales de AWS válidas en `~/.aws/credentials` o variables `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.

---

## Despliegue

### Pipeline (GitHub Actions)

El archivo `.github/workflows/deploy.yml` despliega todas las Lambdas al hacer push a `main`, usando **matrix strategy** para empaquetar e implementar las 5 funciones en paralelo:

```yaml
name: Deploy Lambdas
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        function:
          - lambda-course-service
          - lambda-enrollment-service
          - lambda-task-service
          - lambda-report-generator
          - lambda-notification-worker
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: 20 }
      - name: Install & package
        working-directory: ${{ matrix.function }}
        run: |
          npm ci
          zip -r function.zip . -x '*.git*'
      - name: Deploy Lambda
        run: |
          aws lambda update-function-code \
            --function-name ${{ matrix.function }} \
            --zip-file fileb://${{ matrix.function }}/function.zip
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          AWS_REGION: us-east-1
```

### Secrets necesarios en GitHub

| Secret | Descripción |
|---|---|
| `AWS_ACCESS_KEY_ID` | Credencial de IAM para desplegar |
| `AWS_SECRET_ACCESS_KEY` | Credencial de IAM para desplegar |
| `AWS_REGION` | Región de despliegue |

Las variables `DATABASE_URL`, `SNS_TOPIC_ARN` y `NOTIFICATION_API_KEY` deben estar configuradas **en la función Lambda** (console o CLI), no en el workflow.

---

### Buenas prácticas implementadas

- **Sentencias parametrizadas** (`$1`, `$2`) en todas las consultas SQL para prevenir inyección.
- **Validación de ownership** en operaciones de docentes (`teacher_id === data.teacherId`).
- **Separación de responsabilidades**: el Notification Worker no toca la DB; los servicios de dominio no contactan al Notification API.
- **Logging con `console.error`** en todos los puntos de fallo.
- **IAM Roles por función**: cada Lambda tiene permisos mínimos (p. ej., solo `sns:Publish` sobre el topic).

---

## Recursos

- [AWS Lambda Developer Guide](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html)
- [AWS SDK for JavaScript v3](https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/welcome.html)
- [AWS SNS Developer Guide](https://docs.aws.amazon.com/sns/latest/dg/welcome.html)
- [AWS SQS Developer Guide](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
- [SNS Filter Policies](https://docs.aws.amazon.com/sns/latest/dg/sns-subscription-filter-policies.html)
- [SQS Dead Letter Queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [SQS RedrivePolicy](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SetQueueAttributes.html)
- [Lambda partial batch responses](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html#services-sqs-batchfailurereporting)
- [AWS SAM Developer Guide](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html)
- [Supabase Database Documentation](https://supabase.com/docs/guides/database)
- [Node.js `pg` Documentation](https://node-postgres.com/)

---

## Autor

**Gabriel Emilio** — `gaboemi@duoc.cl`

Asignatura: **DSY1107 Desarrollo Cloud Native**
Evaluación Parcial N° 3 — 2026