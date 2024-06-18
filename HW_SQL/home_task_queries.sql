/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
-- SELECT category.name AS category, COUNT(film_category.film_id) AS count_number
-- FROM film_category 
--     JOIN category 
--         ON film_category.category_id = category.category_id

-- GROUP BY category.category_id

-- ORDER BY count_number DESC;


/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
-- WITH film_rent_count AS (
    
--     SELECT film.film_id, COUNT(rental.rental_id) AS rental_count
--     FROM film
--         JOIN inventory
--             ON film.film_id = inventory.film_id
--         JOIN rental
--             ON inventory.inventory_id = rental.inventory_id
    
--     GROUP BY film.film_id
-- )

-- SELECT CONCAT(MAX(actor.first_name), ' ', MAX(actor.last_name)), SUM(rental_count) AS rental_rate
-- FROM actor 
--     JOIN film_actor 
--         ON actor.actor_id = film_actor.actor_id
--     JOIN film_rent_count
--         ON film_actor.film_id = film_rent_count.film_id

-- GROUP BY actor.actor_id

-- ORDER BY rental_rate DESC

-- LIMIT 10;



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
-- SQL code goes here...

WITH film_rental_revenue AS (
    
    SELECT film.film_id, SUM(payment.amount) AS rental_revenue
    FROM payment
        JOIN rental
            ON payment.rental_id = rental.rental_id
        JOIN inventory
            ON rental.inventory_id = inventory.inventory_id
        JOIN film
            ON inventory.film_id = film.film_id
    
    GROUP BY film.film_id
)

SELECT category.name AS category, SUM(film_rental_revenue.rental_revenue) AS category_rental_revenue
FROM film_rental_revenue
    JOIN film_category
        ON film_rental_revenue.film_id = film_category.film_id
    JOIN category
        ON film_category.category_id = category.category_id

GROUP BY category.category_id

ORDER BY category_rental_revenue DESC;

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
/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
-- SQL code goes here...


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
    