@Client.on_message(filters.command("run") & filters.private)
async def run_forwarding(client, message):
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    usr = await client.get_users(user_id)
    user_nam = f"For @{usr.username}" if usr.username else ""
    if not user or not user.get("accounts"):
        return await message.reply("No userbot account found. Use /add_account first.")

    if user.get("enabled", False):
        return await message.reply("Fᴏʀᴡᴀʀᴅɪɴɢ ᴀʟʀᴇᴀᴅʏ ʀᴜɴɴɪɴɢ. Uꜱᴇ /stop ᴛᴏ ᴇɴᴅ ɪᴛ ʙᴇꜰᴏʀᴇ ꜱᴛᴀʀᴛɪɴɢ ᴀɢᴀɪɴ.")

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Fᴏʀᴡᴀʀᴅ : Sᴀᴠᴇᴅ Mᴇꜱꜱᴀɢᴇ", callback_data="normal"),
            InlineKeyboardButton("Fᴏʀᴡᴀʀᴅ : Wɪᴛʜ Tᴀɢ", callback_data="forward")
        ]
    ])
    choose = await message.reply(
        "Hᴏᴡ ᴅᴏ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ꜱᴇɴᴅ ᴛʜᴇ ᴍᴇꜱꜱᴀɢᴇ?\nCʟɪᴄᴋ ᴏɴ ꜱᴀᴠᴇᴅ ᴍᴇꜱꜱᴀɢᴇ ᴛᴏ ꜱᴇɴᴅ ʟᴀꜱᴛ ᴍᴇꜱꜱᴀɢᴇ ꜱᴀᴠᴇᴅ ʙʏ ᴛʜᴇ ᴜꜱᴇʀ ʙᴏᴛ\nCʟɪᴄᴋ ᴏɴ ᴡɪᴛʜ ᴛᴀɢ ɪꜰ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ꜱᴇɴᴅ ᴍᴇꜱꜱᴀɢᴇ ᴡɪᴛʜ ᴛʜᴇ ꜰᴏʀᴡᴀʀᴅ ᴛᴀɢ [ʏᴏᴜ ʜᴀᴠᴇ ᴛᴏ ɢɪᴠᴇ ᴛʜᴇ ɪɴᴩᴜᴛ ꜰᴏʀ ᴛʜᴀᴛ] \nCʜᴏᴏꜱᴇ ᴀɴ ᴏᴩᴛɪᴏɴ ʙᴇʟᴏᴡ (timeout 60s):",
        reply_markup=keyboard
    )
    try:
        cb: CallbackQuery = await client.listen(user_id, timeout=60)
    except asyncio.exceptions.TimeoutError:
        await choose.delete()
        await message.reply("Tɪᴍᴇ ᴏᴜᴛ, Cʟɪᴄᴋ ᴏɴ /run ᴛᴏ ꜱᴛᴀʀᴛ ᴀɢᴀɪɴ")
        return

    if cb.data.startswith("forward"):
        try:
            user_msg = await client.ask(
                chat_id=user_id,
                text="Send the message you want to save.\n\n**With Tag. Timeout in 5min**",
                timeout=300
            )
            msg = await user_msg.forward(chat_id=Config.MES_CHANNEL)  
            # Store the message ID in DB
            await db.update_user(user_id, {"forward_message_id": msg.message_id})
            await user_msg.delete()
        except asyncio.exceptions.TimeoutError:
            return await message.reply("❌ Timed out. Please start again using /run.")

    await choose.delete()
    syd = await message.reply("Starting....")
    is_premium = user.get("is_premium", False)
    can_use_interval = user.get("can_use_interval", False)

    clients = []
    user_groups = []

    for acc in user["accounts"]:
        session = StringSession(acc["session"])
        tele_client = TelegramClient(session, Config.API_ID, Config.API_HASH)
        await tele_client.start()
        clients.append(tele_client)

        me = await tele_client.get_me()
        session_user_id = me.id

        group_data = await db.group.find_one({"_id": session_user_id}) or {"groups": []}
        groups = group_data["groups"]
        user_groups.append(groups)

    if not any(user_groups):
        await syd.delete()
        return await message.reply("No groups selected. Use /groups to add some.")

    sessions[user_id] = clients
    await db.update_user(user_id, {"enabled": True})
    await syd.delete()
    await message.reply("Forwarding started.")

    account_group_summary = ""

    for i, tele_client in enumerate(clients):
        groups = user_groups[i]
        asyncio.create_task(
            start_forwarding_loop(tele_client, user_id, groups, is_premium, can_use_interval, client, i)
        )
        me = await tele_client.get_me()
        account_name = me.first_name or me.username or "Unknown"
        group_lines = []

        for group in groups:
            try:
                entity = await tele_client.get_entity(group["id"])
                group_title = entity.title if hasattr(entity, "title") else str(group["id"])

                if "topic_id" in group:
                    topics = await tele_client(GetForumTopicsRequest(
                        channel=entity,
                        offset_date=0,
                        offset_id=0,
                        offset_topic=0,
                        limit=100
                    ))
                    topic = next((t for t in topics.topics if t.id == group["topic_id"]), None)
                    if topic:
                        group_title += f" ({topic.title})"
                    else:
                        group_title += f" (Topic ID: {group['topic_id']})"

                group_lines.append(f"  - {group_title}")
            except Exception:
                group_lines.append(f"  - Failed to fetch group {group.get('id')}")

        account_group_summary += f"\n<b>{account_name}</b>:\n" + "\n".join(group_lines) + "\n"

    if account_group_summary.strip():
        await client.send_message(
            user_id,
            f"<b>Accounts and Groups For Forwarding:</b>\n{account_group_summary}\n\nSend /stop to stop the process",
            parse_mode=enums.ParseMode.HTML
        )
    try:
        await client.send_message(
            Config.LOG_CHANNEL,
            f"#Process \n🧊 Fᴏʀᴡᴀʀᴅɪɴɢ ꜱᴛᴀʀᴛᴇᴅ ʙʏ <a href='tg://user?id={user_id}'>{usr.first_name}</a> (User ID: <code>{user_id}</code>)\n\n{account_group_summary}",
            parse_mode=enums.ParseMode.HTML
        )
    except:
        pass




async def start_forwarding_loop(tele_client, user_id, groups, is_premium, can_use_interval, client, index):
    if index > 0:
        await asyncio.sleep(600 * index)  # 10min, if 2, 20min
        await client.send_message(user_id, f"Starting {index}")
    usr = await client.get_users(user_id)
    user_nam = f"For @{usr.username}" if usr.username else ""

    while True:
        interval = 1
        total_slep = 20
        if not (await db.get_user(user_id)).get("enabled", False):
            break

        try:
            if not is_premium:
                meme = await tele_client.get_me()
                expected_name = f"Bot is run by @{temp.U_NAME} " + user_nam
                current_last_name = meme.last_name or ""
                full = await tele_client(GetFullUserRequest(meme.id))
                current_bio = full.full_user.about or ""
                message_lines = ["WARNING: You Have Changed Account Info. [Never Repeat Again. To Remove Ad Get Premium]"]
                update_needed = False
                bio_edit = current_bio

                if current_last_name != expected_name:
                    message_lines.append(f"\nLast name is '{current_last_name}', updating to '{expected_name}'.")
                    update_needed = True

                if current_bio != expected_name:
                    message_lines.append(f"\nBio is '{current_bio}', updating to '{expected_name}'.")
                    update_needed = True
                    bio_edit = expected_name

                if update_needed:
                    await tele_client(UpdateProfileRequest(
                        last_name=expected_name,
                        about=bio_edit
                    ))
                    await client.send_message(user_id, "\n".join(message_lines))
        except Exception as e:
            await client.send_message(user_id, f"Error in Getting Message: {e}")
            print(f"Failed to check user data: {e}")

        try:
            forward_entry = await db.get_user(user_id)
            use_forward = forward_entry.get("forward_message_id", None)
            if use_forward:
                msg_id = forward_entry.get("forward_message_id")
                last_msg = await tele_client.get_messages(entity=Config.MES_CHANNEL, ids=msg_id)
                #use_forward = True
            else:
                last_msg = (await tele_client.get_messages("me", limit=1))[0]
               # use_forward = False
        except Exception as e:
            print(f"Failed to fetch message: {e}")
            await client.send_message(user_id, f"Error in Getting Message: {e}")
            for _ in range(total_slep // interval):
                if not (await db.get_user(user_id)).get("enabled", False):
                    break
                await asyncio.sleep(interval)
            continue

        for grp in groups:
            gid = grp["id"]
            topic_id = grp.get("topic_id")
            interval = grp.get("interval", 300 if (is_premium or can_use_interval) else 7200)
            last_sent = grp.get("last_sent", datetime.min)
            total_wait = interval - (datetime.now() - last_sent).total_seconds()
            if total_wait > 0:
                # Wait total_wait seconds but check every 1 second if enabled
                for _ in range(int(total_wait)):
                    if not (await db.get_user(user_id)).get("enabled", False):
                        break
                    await asyncio.sleep(1)
            try:
                if use_forward:
                    await tele_client.forward_messages(
                        entity=gid,
                        messages=last_msg.id,
                        from_peer=Config.MES_CHANNEL,
                        reply_to=topic_id if topic_id else None
                    )
                else:
                    await tele_client.send_message(
                        gid,
                        last_msg,
                        reply_to=topic_id if topic_id else None
                    )
                grp["last_sent"] = datetime.now()
                me = await tele_client.get_me()
                await db.group.update_one({"_id": me.id}, {"$set": {"groups": groups}})
                await db.user_messages.insert_one({
                    "user_id": user_id,
                    "group_id": gid,
                    "time": datetime.now(tz=india)
                    })
            except Exception as e:
                print(f"Error sending to {gid}: {e}")
                await client.send_message(user_id, f"Error sending to {gid}:\n{e} \nSend This Message To The Admin, To Take Proper Action, Forwarding Won't Stop.[Never Let The Account Get Banned Due To Spam]")

        for _ in range(total_slep // interval):
            if not (await db.get_user(user_id)).get("enabled", False):
                break
            await asyncio.sleep(interval)

    await client.send_message(user_id, "Sᴛᴏᴩᴩᴇᴅ..!")
    await db.update_user(user_id, {"forward_message_id": None})
    syd = await client.send_message(user_id, "Sᴇɴᴅɪɴɢ ꜰᴏʀᴡᴀʀᴅ ᴅᴀᴛᴀ...")

    entries = await db.user_messages.find({"user_id": user_id}).to_list(None)
    if not entries:
        return await syd.edit("No forwarding data found for this user.")

    grouped = defaultdict(list)
    for entry in entries:
        group_id = entry.get("group_id")
        timestamp = entry.get("time")
        if isinstance(timestamp, datetime):
            timestamp = timestamp.astimezone(india)
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S IST")
        else:
            timestamp_str = str(timestamp)
        grouped[group_id].append(timestamp_str)
    for group_id in grouped:
        grouped[group_id].sort()
    out = f"User ID: {user_id}\n"
    for group_id, times in grouped.items():
        out += f"  => Group ID: {group_id}\n"
        for t in times:
            out += f"    - {t}\n"
    with open("forward.txt", "w", encoding="utf-8") as f:
        f.write(out)

    await client.send_document(user_id, "forward.txt", caption=f"Fᴏʀᴡᴀʀᴅ ʟᴏɢꜱ")
    await db.user_messages.delete_many({"user_id": user_id})
    await syd.delete()

