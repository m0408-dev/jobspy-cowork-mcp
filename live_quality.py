import asyncio,json
from discovery import employer_page
async def main():
 for provider,slug in [('ashby','Ashby'),('smartrecruiters','BoschGroup')]:
  try:
   jobs,cursor=await employer_page(provider,slug)
   print(json.dumps({'provider':provider,'jobs':len(jobs),'descriptions':sum(bool(j.get('description')) for j in jobs),'remote_flags':sum(j.get('is_remote') is True for j in jobs),'next_cursor':cursor}))
   if cursor is not None:
    following,next_cursor=await employer_page(provider,slug,cursor=cursor)
    print(json.dumps({'provider':provider,'second_page':len(following),'overlap':len({j['job_url'] for j in jobs}&{j['job_url'] for j in following}),'next_cursor':next_cursor}))
  except Exception as e:print(provider,type(e).__name__,str(e)[:150])
asyncio.run(main())
