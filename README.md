# Торговая стратегия: Momentum

**Автор:** [Шилов Игорь]
**Проектный документ:** (docs/project-description.md)

## Управление проектом
- **План работ (Task Tracker):** [Ссылка на проект GitHub Projects](https://github.com/orgs/omsu-informational-systems/projects/1)

## Назначение сервиса
Сервис предназначен для автоматического совершения сделок на платформе с использованием импульсной стратегии.

## Требования к окружению
- Установленный Docker c docker-compose.

1. **Сборка Docker-образа:**
   Находясь в корневой папке репозитория, выполните команду:
   ```bash
   docker compose up -d --build

2. **Корректное завершение**
   ```bash
   docker compose down