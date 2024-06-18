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
