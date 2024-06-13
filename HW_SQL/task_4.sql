/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
-- SQL code goes here...

SELECT DISTINCT film.title
FROM film

EXCEPT

SELECT DISTINCT film.title
FROM inventory
    JOIN film
        ON inventory.film_id = film.film_id;
