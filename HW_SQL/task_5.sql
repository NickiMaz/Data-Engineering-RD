/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/

SELECT CONCAT(MAX(actor.first_name), ' ', MAX(actor.last_name)), COUNT(film_actor.film_id) AS film_count
FROM actor 
    JOIN film_actor
        ON actor.actor_id = film_actor.actor_id

WHERE film_actor.film_id IN (

    SELECT film.film_id
    FROM category 
        JOIN film_category
            ON category.category_id = film_category.category_id
        JOIN film
            ON film_category.film_id = film.film_id
    WHERE category.name = 'Children'
)

GROUP BY actor.actor_id

ORDER BY film_count DESC

LIMIT 3;
