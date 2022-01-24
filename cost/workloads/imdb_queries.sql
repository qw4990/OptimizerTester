CREATE TABLE `cast_info` (      -- 36244344
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

CREATE TABLE `movie_companies` (        -- 2609129
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

CREATE TABLE `movie_keyword` (      -- 4523930
  `id` int(11) NOT NULL,
  `movie_id` int(11) NOT NULL,
  `keyword_id` int(11) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `keyword_id_movie_keyword` (`keyword_id`),
  KEY `movie_id_movie_keyword` (`movie_id`)
);

CREATE TABLE `title` (      -- 2528312
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

-- ########################## evaluation queries ##########################

-- SCAN
SELECT /*+ use_index(cast_info, primary) */ id FROM cast_info WHERE id>=? AND id<=?; -- table scan
SELECT /*+ use_index(cast_info, movie_id_cast_info) */ movie_id FROM cast_info WHERE movie_id>=? AND movie_id<=?; -- index scan

-- DESC SCAN
SELECT /*+ use_index(cast_info, primary), no_reorder() */ id FROM cast_info WHERE id>=? AND id<=? ORDER BY id DESC; -- table scan
SELECT /*+ use_index(cast_info, movie_id_cast_info), no_reorder() */ movie_id FROM cast_info WHERE movie_id>=? AND movie_id<=? ORDER BY movie_id DESC; -- index scan

-- LOOKUP
SELECT /*+ use_index(movie_companies, movie_id_movie_companies) */ * FROM movie_companies WHERE movie_id>=? AND movie_id<=?; -- lookup

-- AGG
SELECT /*+ use_index(movie_companies, company_id_movie_companies), stream_agg(), agg_to_cop() */ COUNT(*) FROM movie_companies WHERE company_id>=? AND company_id<=?;
SELECT /*+ use_index(movie_companies, company_id_movie_companies), stream_agg(), agg_not_to_cop() */ COUNT(*) FROM movie_companies WHERE company_id>=? AND company_id<=?;

-- SORT
SELECT /*+ use_index(movie_companies, movie_id_movie_companies), must_reorder() */ movie_id FROM movie_companies WHERE movie_id>=? AND movie_id<=? ORDER BY movie_id; -- sort 
SELECT /*+ use_index(movie_companies, company_id_movie_companies), must_reorder() */ company_id FROM movie_companies WHERE company_id>=? AND company_id<=? ORDER BY company_id; -- sort 
