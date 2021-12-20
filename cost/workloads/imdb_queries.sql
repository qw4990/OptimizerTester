CREATE TABLE `cast_info` (
  `id` int(11) NOT NULL,
  `person_id` int(11) NOT NULL,
  `movie_id` int(11) NOT NULL,
  `person_role_id` int(11) DEFAULT NULL,
  `note` text DEFAULT NULL,
  `nr_order` int(11) DEFAULT NULL,
  `role_id` int(11) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `movie_id_cast_info` (`movie_id`),
  KEY `person_id_cast_info` (`person_id`),
  KEY `person_role_id_cast_info` (`person_role_id`),
  KEY `role_id_cast_info` (`role_id`)
);

SELECT * FROM cast_info WHERE movie_id = ?;                         -- find the cast info of a movie
SELECT * FROM cast_info WHERE person_id = ?;                        -- find the cast info of a person
SELECT * FROM cast_info WHERE person_id = ? AND movie_id = ?;       -- find the cast info of a person in a movie
SELECT * FROM cast_info WHERE person_id = ? AND person_role_id = ?; -- find the cast info of a person with a role


CREATE TABLE `company_name` (
  `id` int(11) NOT NULL,
  `name` varchar(512) NOT NULL,
  `country_code` varchar(6) DEFAULT NULL,
  `imdb_id` int(11) DEFAULT NULL,
  `name_pcode_nf` varchar(5) DEFAULT NULL,
  `name_pcode_sf` varchar(5) DEFAULT NULL,
  `md5sum` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
);

SELECT * FROM company_name WHERE id = ?;                            -- find a company
SELECT * FROM company_name WHERE name = ?;                          -- find a company by the name
SELECT * FROM company_name WHERE country_code = ?;                  -- find all companies with a code



CREATE TABLE `movie_companies` (
  `id` int(11) NOT NULL,
  `movie_id` int(11) NOT NULL,
  `company_id` int(11) NOT NULL,
  `company_type_id` int(11) NOT NULL,
  `note` text DEFAULT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `company_id_movie_companies` (`company_id`),
  KEY `company_type_id_movie_companies` (`company_type_id`),
  KEY `movie_id_movie_companies` (`movie_id`)
);

SELECT * FROM movie_companies WHERE movie_id = ?;                   -- find the company of a movie
SELECT * FROM movie_companies WHERE company_id = ?;                 -- find movies of a company

CREATE TABLE `movie_keyword` (
  `id` int(11) NOT NULL,
  `movie_id` int(11) NOT NULL,
  `keyword_id` int(11) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `keyword_id_movie_keyword` (`keyword_id`),
  KEY `movie_id_movie_keyword` (`movie_id`)
);

SELECT * FROM movie_keyword WHERE movie_id = ?;                     -- find all keywords of a movie
SELECT * FROM movie_keyword WHERE keyword_id = ?;                   -- find all movies with the keyword


CREATE TABLE `title` (
  `id` int(11) NOT NULL,
  `title` varchar(512) NOT NULL,
  `imdb_index` varchar(5) DEFAULT NULL,
  `kind_id` int(11) NOT NULL,
  `production_year` int(11) DEFAULT NULL,
  `imdb_id` int(11) DEFAULT NULL,
  `phonetic_code` varchar(5) DEFAULT NULL,
  `episode_of_id` int(11) DEFAULT NULL,
  `season_nr` int(11) DEFAULT NULL,
  `episode_nr` int(11) DEFAULT NULL,
  `series_years` varchar(49) DEFAULT NULL,
  `md5sum` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `kind_id_title` (`kind_id`)
);


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

