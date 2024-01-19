## Описание
Для работы возьмем реальные данные о выполненных авиа-рейсах (2015 Flight Delays and Cancellations). Для упрощения будем работать с 50% от основной выборки и для практики используем следующие датасеты:

1. airlines.parquet- файл с данными о авиалиниях
2. airports.parquet - файл с данными о аэропортах
3. flights.parquet - файл с данными о авиарейсах


### Структура данных
#### Авиалинии

Колонка   |Тип     | Описание
----------|--------|------------------------
IATA_CODE | String | Идентификатор авиалинии
AIRLINE   | String | Название авиалинии

#### Аэропорты

Колонка   | Тип    | Описание
----------|--------|------------------------
IATA_CODE | String | Идентификатор аэропорта
AIRPORT   | String | Название аэропорта
CITY      | String | Город
STATE     | String | Штат/Округ
COUNTRY   | String | Страна
LATITUDE  | Float  | Широта расположения
LONGITUDE | Float  | Долгота расположения

#### Авиарейсы

Колонка | Тип | Описание
--------------------|--------|------------------------
YEAR | Integer | Год полета
MONTH | Integer | Месяц полета
DAY | Integer | День полета
DAY_OF_WEEK | Integer | День недели полета [1-7] = [пн-вс]
AIRLINE | String | Код авиалиний
FLIGHT_NUMBER | String | Идентификатор рейса (просто ид)
TAIL_NUMBER | String | Номер рейса
ORIGIN_AIRPORT | String | Код аэропорта отправления
DESTINATION_AIRPORT | String | Код аэропорта назначения
SCHEDULED_DEPARTURE | Integer | Время запланированного отправления
DEPARTURE_TIME | Integer | Время фактического отправления WHEEL_OFF - TAXI_OUT
DEPARTURE_DELAY | Integer | Общая задержка отправления
TAXI_OUT | Integer | Время, прошедшее между отправлением от выхода на посадку в аэропорту отправления и вылетом
WHEELS_OFF | Integer | Момент времени, когда колеса самолета отрываются от земли
SCHEDULED_TIME | Integer | Запланированное количество времени, необходимое для полета
ELAPSED_TIME | Integer | AIR_TIME+TAXI_IN+TAXI_OUT
AIR_TIME | Integer | Время в воздухе. Промежуток времени между WHEELS_OFF и WHEELS_ON
DISTANCE | Integer | Расстояние между двумя аэропортами
WHEELS_ON | Integer | Момент времени, когда колеса самолета касаются земли
TAXI_IN | Integer | Время, прошедшее между посадкой на колеса и прибытием на посадку в аэропорту назначения
SCHEDULED_ARRIVAL | Integer | Планируемое время прибытия
ARRIVAL_TIME | Integer | Время когда самолет фактически прибыл в аэропорт (прибыл к гейту) WHEELS_ON+TAXI_IN
ARRIVAL_DELAY | Integer | Время задержки в прибытии ARRIVAL_TIME-SCHEDULED_ARRIVAL
DIVERTED | Integer | Флаг указывающий что рейс приземлился в аэропорту не по расписанию (0/1)
CANCELLED | Integer | Флаг указывающий что рейс был отменен (0/1)
CANCELLATION_REASON | String | Причина отмены рейса: A - Airline/Carrier; B - Weather; C - National Air System; D - Security
AIR_SYSTEM_DELAY | Integer | Время задержки из-за воздушной системы
SECURITY_DELAY | Integer | Время задержки из-за службы безопасности
AIRLINE_DELAY | Integer | Время задержки по вине авиакомпании
LATE_AIRCRAFT_DELAY | Integer | Время задержки из-за проблем самолета
WEATHER_DELAY | Integer | Время задержки из-за погодных условий


> # Задача №1

Постройте сводную таблицу отображающую топ 10 рейсов по коду рейса (TAIL_NUMBER) и числу вылетов за все время. Отсеките значения без указания кода рейса.

Пример вывода:
TAIL_NUMBER | count
------------|------
N480HA | 1763
N484HA | 1654
N481HA | 1434

Сохраните сводную таблицу в формате parquet. 


> # Задача №2

Найдите топ 10 авиамаршрутов (ORIGIN_AIRPORT, DESTINATION_AIRPORT) по наибольшему числу рейсов, а так же посчитайте среднее время в полете (AIR_TIME).

Требуемые поля:
Колонка | Описание
--------|---------
ORIGIN_AIRPORT | Аэропорт вылета
DESTINATION_AIRPORT | Аэропорт прибытия
tail_count | Число рейсов по маршруту (TAIL_NUMBER)
avg_air_time | среднее время в небе по маршруту



> # Задача №3

Аналитик попросил определить список аэропортов у которых самые больше проблемы с задержкой на вылет рейса. Для этого необходимо вычислить среднее, минимальное, максимальное время задержки и выбрать аэропорты только те где максимальная задержка (DEPARTURE_DELAY) 1000 секунд и больше. Дополнительно посчитать корреляцию между временем задержки и днем недели (DAY_OF_WEEK)

Требуемые поля:
Поле | Описание
-----|---------
ORIGIN_AIRPORT | Код аэропорта отправления
avg_delay | Среднее время задержки для аэропорта
min_delay | Минимальное время задержки для аэропорта
max_delay | Максимальное время задержки для аэропорта
corr_delay2day_of_week | Корреляция между временем задержки и днем недели

> # Задача №4

Для дашборда с отображением выполненных рейсов требуется собрать таблицу на основе наших данных.

Требуемые поля:
Поле | Описание
-----|---------
AIRLINE_NAME | Название авиалинии (airlines.AIRLINE)
TAIL_NUMBER | Номер рейса (flights.TAIL_NUMBER)
ORIGIN_COUNTRY | Страна отправления (airports.COUNTRY)
ORIGIN_AIRPORT_NAME | Полное название аэропорта отправления (airports.AIRPORT)
ORIGIN_LATITUDE | Широта аэропорта отправления (airports.LATITUDE)
ORIGIN_LONGITUDE | Долгота аэропорта отправления (airports.LONGITUDE)
DESTINATION_COUNTRY | Страна прибытия (airports.COUNTRY)
DESTINATION_AIRPORT_NAME | Полное название аэропорта прибытия (airports.AIRPORT)
DESTINATION_LATITUDE | Широта аэропорта прибытия (airports.LATITUDE)
DESTINATION_LONGITUDE | Долгота аэропорта прибытия (airports.LONGITUDE)


> # Задача №5

Отдел аналитики интересует статистика по компаниям о возникших проблемах. Пришла задача построить сводную таблицу о всех авиакомпаниях содержащую следующие данные:
Колонка | Описание
--------|----------
AIRLINE_NAME | полное название авиакомпании
correct_count | число выполненных рейсов
diverted_count  | число рейсов выполненных с задержкой
cancelled_count | число отмененных рейсов
avg_distance | средняя дистанция рейсов
avg_air_time | среднее время в небе
airline_issue_count | число задержек из-за проблем с самолетом [CANCELLATION_REASON]
weather_issue_count | число задержек из-за погодных условий [CANCELLATION_REASON]
nas_issue_count | число задержек из-за проблем NAS [CANCELLATION_REASON]
security_issue_count | число задержек из-за службы безопасности [CANCELLATION_REASON]
