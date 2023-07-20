### Задача.
Необходима автоматизированный сбор, обработка и выгрузка в человеко читаемом формате файлов(XLSX) исполнителям(работникам).
А так же обработка и сбор исходящих реестров ГКО.

### Логика решения.
Сбор информации производимой при проведении ГКО:

Входящая информация:
1. Входящая информация поступает из Росреестра на почтовый ящик MS Exchange.
2. Файлы вложений и ссылки на вложения(mail cloud) скачиваются, для обработки.
3. Происходит загрузка XML в таблицы БД PG.
4. По завершению из БД формируется выгрузка в формате XLSX.
5. Выгрузка отправляется на почтовый ящик.

Исходящая информация:
1. Готовые файлы выгрузок фиксируются в shared folder и скачиваются, для обработки.
2. Файлы XML выгрузок загружаются и происходит вся необходимая обработка в БД.


### Структура проекта
- airflow: даги, докер и скрипты необходимые в первичной настройки и загрузки;
- формат вохдные данные: zip/rar архив файлов XML в соответствии со схемой;
- schema: схема XML Росреестра;
- источник: MS Exchange;
- выгрузки: человеко читаемый формат xlsx;
- целевые системы: postgres & shared folder;
- migrations.sql: миграция из старой БД.

### Запуск проекта

Для запуска проекта использовать docker-compose.yml. И выполнить скрипты инициализации: Makefile(start)