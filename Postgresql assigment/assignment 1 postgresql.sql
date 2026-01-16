SELECT title, release_year FROM film;

SELECT COUNT(*) AS total_films FROM film;

SELECT DISTINCT c.first_name, c.last_name
FROM customer c
JOIN rental r ON c.customer_id = r.customer_id;

SELECT f.title
FROM film f
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
WHERE c.name = 'Action';

SELECT c.name, COUNT(fc.film_id) AS film_count
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
GROUP BY c.name;

SELECT title, length
FROM film
ORDER BY length DESC
LIMIT 5;

SELECT COUNT(*) 
FROM rental
WHERE rental_date BETWEEN '2006-01-01' AND '2006-01-31';

SELECT title, rental_rate
FROM film
WHERE rental_rate > 3;

SELECT DISTINCT city
FROM city c
JOIN address a ON c.city_id = a.city_id
JOIN customer cu ON a.address_id = cu.address_id;

SELECT DISTINCT s.first_name, s.last_name
FROM staff s
JOIN rental r ON s.staff_id = r.staff_id;


Section B: Intermediate Queries (11–20)

SELECT f.title, COUNT(r.rental_id) AS rentals
FROM film f
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY f.title
ORDER BY rentals DESC
LIMIT 10;

SELECT f.title, SUM(p.amount) AS revenue
FROM film f
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
JOIN payment p ON r.rental_id = p.rental_id
GROUP BY f.title;

SELECT c.first_name, c.last_name, COUNT(r.rental_id) AS rentals
FROM customer c
JOIN rental r ON c.customer_id = r.customer_id
GROUP BY c.customer_id
HAVING COUNT(r.rental_id) > 20;

SELECT c.name, AVG(f.rental_duration) AS avg_duration
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
JOIN film f ON fc.film_id = f.film_id
GROUP BY c.name;

SELECT DISTINCT f.title
FROM film f
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
WHERE r.return_date <= r.rental_date + f.rental_duration * INTERVAL '1 day';

SELECT c.name, COUNT(*) AS rentals
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
JOIN inventory i ON fc.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY c.name
ORDER BY rentals DESC
LIMIT 1;

SELECT DATE_TRUNC('month', rental_date) AS month, COUNT(*)
FROM rental
WHERE rental_date BETWEEN '2005-01-01' AND '2006-12-31'
GROUP BY month
ORDER BY month;

SELECT c.customer_id, c.first_name, c.last_name
FROM customer c
JOIN rental r ON c.customer_id = r.customer_id
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film_category fc ON i.film_id = fc.film_id
GROUP BY c.customer_id
HAVING COUNT(DISTINCT fc.category_id) > 5;

SELECT DISTINCT f.title
FROM film f
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
JOIN customer c ON r.customer_id = c.customer_id
JOIN address a ON c.address_id = a.address_id
JOIN city ci ON a.city_id = ci.city_id
JOIN country co ON ci.country_id = co.country_id
WHERE co.country = 'Canada';

SELECT c.first_name, c.last_name, SUM(p.amount) AS total_paid
FROM customer c
JOIN payment p ON c.customer_id = p.customer_id
GROUP BY c.customer_id
ORDER BY total_paid DESC
LIMIT 5;

Section C: Advanced Queries (21–30)

SELECT f.title,
       COUNT(r.rental_id) AS rentals,
       RANK() OVER (ORDER BY COUNT(r.rental_id) DESC) AS rank
FROM film f
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY f.title;

WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', payment_date) AS month,
        SUM(amount) AS total_revenue
    FROM payment
    GROUP BY DATE_TRUNC('month', payment_date)
)
SELECT
    month,
    total_revenue
FROM monthly_revenue
ORDER BY total_revenue DESC
LIMIT 3;

SELECT
    f.title,
    SUM(p.amount) AS film_revenue
FROM film f
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
JOIN payment p ON r.rental_id = p.rental_id
GROUP BY f.title
HAVING SUM(p.amount) >
       (SELECT AVG(amount) FROM payment);

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(p.amount) AS lifetime_value
FROM customer c
JOIN payment p ON c.customer_id = p.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY lifetime_value DESC;

SELECT DISTINCT f.title
FROM film f
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY f.title
HAVING COUNT(DISTINCT DATE_TRUNC('month', r.rental_date)) > 1;


CREATE MATERIALIZED VIEW film_popularity_by_category AS
SELECT
    c.name AS category,
    COUNT(r.rental_id) AS rental_count
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
JOIN inventory i ON fc.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY c.name;

REFRESH MATERIALIZED VIEW film_popularity_by_category;

SELECT * FROM film_popularity_by_category;

CREATE OR REPLACE FUNCTION get_film_revenue(p_film_id INT)
RETURNS NUMERIC AS $$
DECLARE
    total_revenue NUMERIC;
BEGIN
    SELECT SUM(p.amount)
    INTO total_revenue
    FROM payment p
    JOIN rental r ON p.rental_id = r.rental_id
    JOIN inventory i ON r.inventory_id = i.inventory_id
    WHERE i.film_id = 1;

    RETURN total_revenue;
END;
$$ LANGUAGE plpgsql;

SELECT get_film_revenue(1);



CREATE TABLE IF NOT EXISTS rental_late_log (
    log_id SERIAL PRIMARY KEY,
    rental_id INT,
    return_date DATE,
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE OR REPLACE FUNCTION log_late_rental()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.return_date IS NOT NULL
       AND NEW.return_date > NEW.rental_date + INTERVAL '5 days' THEN
       
        INSERT INTO rental_late_log (rental_id, return_date)
        VALUES (NEW.rental_id, NEW.return_date);
    END IF;

    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_late_rental ON rentals;
CREATE TRIGGER trg_late_rental
AFTER UPDATE OF return_date
ON rental
FOR EACH ROW
EXECUTE FUNCTION log_late_rental();
UPDATE rental
SET return_date = CURRENT_DATE
WHERE rental_id = 1;
SELECT * FROM rental_late_log;

SELECT
    EXTRACT(YEAR FROM r.rental_date) AS rental_year,
    SUM(p.amount) AS yearly_revenue
FROM rental r
JOIN payment p
    ON r.rental_id = p.rental_id
GROUP BY EXTRACT(YEAR FROM r.rental_date)
ORDER BY rental_year;

SELECT *
FROM film
WHERE title = 'Academy Dinosaur';
CREATE INDEX idx_film_title
ON film(title);
SELECT *
FROM film
WHERE title = 'Academy Dinosaur';



