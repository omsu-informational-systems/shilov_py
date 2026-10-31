# Торговая стратегия: Momentum

**Автор:** [Шилов Игорь]
**Проектный документ:** (docs/project-description.md)

## Управление проектом
- **План работ (Task Tracker):** [Ссылка на проект GitHub Projects](https://github.com/orgs/omsu-informational-systems/projects/1)

## Назначение сервиса
Сервис предназначен для автоматического совершения сделок на платформе с использованием импульсной стратегии.

## Требования к окружению
- Установленный Docker

1. **Сборка Docker-образа:**
   Находясь в корневой папке репозитория, выполните команду:
   ```bash
   docker run -d -p 8000:8000 --env-file .env --name momentum-bot momentum-service