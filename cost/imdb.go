package main

/*
SELECT * FROM cast_info WHERE movie_id = ?;                         -- find the cast info of a movie
SELECT * FROM cast_info WHERE person_id = ?;                        -- find the cast info of a person
SELECT * FROM cast_info WHERE person_id = ? AND movie_id = ?;       -- find the cast info of a person in a movie
SELECT * FROM cast_info WHERE person_id = ? AND person_role_id = ?; -- find the cast info of a person with a role

SELECT * FROM company_name WHERE id = ?;                            -- find a company
SELECT * FROM company_name WHERE name = ?;                          -- find a company by the name
SELECT * FROM company_name WHERE country_code = ?;                  -- find all companies with a code

SELECT * FROM movie_companies WHERE movie_id = ?;                   -- find the company of a movie
SELECT * FROM movie_companies WHERE company_id = ?;                 -- find movies of a company

SELECT * FROM movie_keyword WHERE movie_id = ?;                     -- find all keywords of a movie
SELECT * FROM movie_keyword WHERE keyword_id = ?;                   -- find all movies with the keyword

SELECT * FROM title WHERE id = ?;                                                                   -- find the title of a movie
SELECT * FROM title WHERE title = ?;                                                                -- find a movie by its title
SELECT * FROM title WHERE production_year = ?;                                                      -- find movies by year
SELECT * FROM title WHERE production_year BETWEEN ? AND ?;                                          -- find movies by year
SELECT * FROM title WHERE production_year BETWEEN ? AND ? ORDER BY production_year;                 -- find movies by year
SELECT COUNT(*) FROM title WHERE production_year BETWEEN ? AND ?;                                   -- find movies by year
SELECT * FROM title WHERE episode_nr = ?;                                                           -- find movies by episode_nr
SELECT * FROM title WHERE episode_nr BETWEEN ? AND ?;                                               -- find movies by episode_nr
SELECT * FROM title WHERE episode_nr BETWEEN ? AND ? ORDER BY episode_nr;                           -- find movies by episode_nr
SELECT COUNT(*) FROM title WHERE episode_nr BETWEEN ? AND ?;                                        -- find movies by episode_nr
*/

func genIMDBQueries() []string {

}
