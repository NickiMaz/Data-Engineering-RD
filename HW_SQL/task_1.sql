/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT category.name AS category, COUNT(film_category.film_id) AS count_number
FROM film_category
    RIGHT JOIN category 
        ON film_category.category_id = category.category_id

GROUP BY category.category_id

ORDER BY count_number DESC;
