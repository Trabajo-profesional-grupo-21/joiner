

def update(db, user_id: str, file_name: str, new_data: dict, type: str = 'video'):
    try:
        file_name = f"{user_id}-{file_name}"
        if type == "video":
            data = db["data"].find_one_and_update({"user_id": user_id, "file_name": file_name}, 
                                                        {"$set": {f"data.{new_data['batch_id']}": new_data["batch"]}}, return_document=True)
        else:
            data = db["data"].find_one_and_update({"user_id": user_id, "file_name": file_name}, 
                                                        {"$set": {"data": new_data["batch"]}}, return_document=True)
        if data is None:
            raise Exception("File Not Found")

        return data
    except Exception as e:
         raise Exception(f"Database Error [{e}]")
