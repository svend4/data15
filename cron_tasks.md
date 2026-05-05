# Orchestrator Cron Tasks

## Описание
Этот файл описывает scheduled tasks для Orchestrator.
Формат: cron_expression | description | output_path

## Scheduled Tasks

### Daily Standup (Hermes)
0 9 * * 1-5 | Подготовить ежедневный summary задач | /workspace/orchestrator/reports/daily_{date}.md

### Weekly Report (ReportWriter)
0 18 * * 5 | Подготовить еженедельный отчёт | /workspace/orchestrator/reports/weekly_{date}.md

### Health Check (OpenClaw)
0 */4 * * * | Проверить доступность сервисов | /workspace/orchestrator/logs/health_{date}.log

### Cleanup (System)
0 2 * * 0 | Очистка старых логов | /workspace/orchestrator/logs/cleanup.log

## Активные Cron Jobs

(Фактические cron jobs управляются через create_cron_job tool)