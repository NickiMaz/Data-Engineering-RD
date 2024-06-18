/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
WITH film_rent_count AS (
    
    SELECT film.film_id, COUNT(rental.rental_id) AS rental_count
    FROM film
        JOIN inventory
            ON film.film_id = inventory.film_id
        JOIN rental
            ON inventory.inventory_id = rental.inventory_id
    
    GROUP BY film.film_id
)

SELECT CONCAT(MAX(actor.first_name), ' ', MAX(actor.last_name)), SUM(rental_count) AS rental_rate
FROM actor 
    JOIN film_actor 
        ON actor.actor_id = film_actor.actor_id
    JOIN film_rent_count
        ON film_actor.film_id = film_rent_count.film_id

GROUP BY actor.actor_id

ORDER BY rental_rate DESC
LIMIT 10-;
