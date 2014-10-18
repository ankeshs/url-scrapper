select s.id, s.url
from stories as s left join story_fetch as sf
on s.id=sf.story_id
where (sf.locked=0 or sf.locked is null) and s.flag=1
order by sf.update_count asc, sf.last_update asc, s.id desc
limit 50;