

select 
  movieId,
  userid,
  count(*)
from 
  `scenic-genre-422311-g5.ods_mrs.src_rating_data`
group by 
  movieId, userid
having 
  count(*) >1;


select
  movie.id,
  movie.original_title as movie_name,
  movie.genres,
  movie.release_date,
  subRating.imdb_rating
from
(
  select 
    rating.movieId,
    avg(rating.rating) as imdb_rating
  from
     `scenic-genre-422311-g5.ods_mrs.src_rating_data` rating
  group by rating.movieId

) subRating
inner join `scenic-genre-422311-g5.ods_mrs.src_movie_data` movie
on(subRating.movieId=movie.id)
