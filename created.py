import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from bson.binary import Binary, UUID_SUBTYPE

import uuid
from Settings import settings
from typing import List
from SkillsCreatedSummary import SkillsCreatedSummary


MONGO_URI = settings.MONGODB_URI

SOURCE_DB_NAME = "htd-core-ms"
REPORT_DB_NAME = "by-reporting"
REPORT_COLLECTION = "rep_skills_created"

client = AsyncIOMotorClient(MONGO_URI, uuidRepresentation="standard")
source_db = client[SOURCE_DB_NAME]
report_db = client[REPORT_DB_NAME]


def convert_to_models(raw_results: List[dict]) -> List[SkillsCreatedSummary]:
    """
    Converts raw MongoDB workspace dicts into WorkspaceSummary model instances.

    Args:
        raw_results (List[dict]): The list of documents returned by the aggregation.

    Returns:
        List[SkillsCreatedSummary]: List of parsed and validated workspace summaries.
    """
    return [SkillsCreatedSummary(**doc) for doc in raw_results]

async def fetch_skills_created_by_month(org_uuid_str: str) -> List[SkillsCreatedSummary]:
    org_id_bin = Binary(uuid.UUID(org_uuid_str).bytes, UUID_SUBTYPE)
    pipeline = [
        {
            "$match": { #מוצא את האלו שיש להם id כמו מה שנישלח
                "org_id": org_id_bin,
                "created_at": {"$exists": True}
            }
        },
        { #משאיר רק את מה שרוצים ויוצר שורה חדשה של שנה-חודש
            "$project": {
                "org_id": 1,
                "created_at": 1,
                "year_month": {
                    "$dateToString": {
                        "format": "%Y-%m",
                        "date": "$created_at"
                    }
                }
            }
        },
        { #נמרף לכל מסמך את בשם של האירגון מקלוקשין אחר
            "$lookup": {
                "from": "organizations",
                "localField": "org_id",
                "foreignField": "id",
                "as": "org_info"
            }
        },
        { #מפצל את זה לשורה (הגיע ממערך)
            "$unwind": "$org_info"
        },
        {
            "$group": { #סופר כמה skills נוצר באותו חודש
                "_id": {
                    "org_id": "$org_id",
                    "org_name": "$org_info.name",
                    "year_month": "$year_month"
                },
                "created_skills": {"$sum": 1}
            }
        },
        {
            "$project": { #מסדר את זה לפי מה שרצינו
                "_id": 0,
                "org_id": "$_id.org_id",
                "org_name": "$_id.org_name",
                "year_month": "$_id.year_month",
                "created_skills": 1
            }
        },
        {#גורם לסדר להיותר מהכי קרוב לרחוק
            "$sort": {
                "year_month": -1
            }
        }
    ]

    cursor = source_db["skills"].aggregate(pipeline)
    results = await cursor.to_list(length=None)
    return convert_to_models(results)



async def insert_report(summaries: List[SkillsCreatedSummary]):
    if not summaries:
        print("No summaries to insert.")
        return

    delete_result = await report_db[REPORT_COLLECTION].delete_many({})
    print(f"Deleted entire collection: {delete_result.deleted_count} documents")

    docs = [s.model_dump() for s in summaries]
    try:
        result = await report_db[REPORT_COLLECTION].insert_many(docs, ordered=False)
        print(f"Inserted {len(result.inserted_ids)} documents into {REPORT_COLLECTION}")
    except Exception as e:
        print(f"Failed to insert documents: {e}")


    # for summary in summaries:
    #     filter_query = {
    #         "org_id": summary.org_id,
    #         "year_month": summary.year_month
    #     }
    #     update_doc = {
    #         "$set": summary.model_dump()
    #     }
    #     await report_db[REPORT_COLLECTION].update_one(
    #         filter_query,
    #         update_doc,
    #         upsert=True
    #     )
    # print(f"Upserted {len(summaries)} documents into {REPORT_COLLECTION}")


async def main():
    org_uuid_str = '006de98a-131c-4847-92a9-3150033d6a35'
    summaries = await fetch_skills_created_by_month(org_uuid_str)
    await insert_report(summaries)
    for summary in summaries:
        print(summary.model_dump())


if __name__ == "__main__":
    asyncio.run(main())


