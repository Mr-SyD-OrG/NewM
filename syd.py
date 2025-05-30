@Client.on_message(filters.command("run") & filters.private)
async def run_forwarding(client, message):
    user_id = message.from_user.id
    user = await db.get_user(user_id)
    usr = await client.get_users(user_id)
    user_nam = f"For @{usr.username}" if usr.username else ""
    if not user or not user.get("accounts"):
        return await message.reply("No userbot account found. Use /add_account first.")

    if user.get("enabled", False):
        return await message.reply("Forwarding already running. Use /stop to end it before starting again.")

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📩 Normal Message", callback_data="normal"),
            InlineKeyboardButton("🔁 Forward Tag", callback_data="forward")
        ]
    ])
    await message.reply(
        "How do you want to forward messages? Choose an option below (timeout 30s):",
        reply_markup=keyboard
    )

    forward_message_id = None

    try:
        cb: CallbackQuery = await client.listen(user_id, timeout=60)
    except asyncio.exceptions.TimeoutError:
        return

    if cb.data.startswith("forward"):
        try:
            user_msg = await client.ask(
                chat_id=user_id,
                text="Send the message you want to save.\n\n**Don't add extra text — it will be treated as ad text.**",
                timeout=300
            )
            log_msg = await user_msg.copy(
                chat_id=Config.LOG_CHANNEL,
                caption="📝 Forward Tag Message",
                parse_mode=enums.ParseMode.HTML
            )
            # Store the message ID in DB
            await db.update_user(user_id, {"forward_message_id": log_msg.message_id})
        except asyncio.exceptions.TimeoutError:
            return await message.reply("❌ Timed out. Please start again using /run.")

    syd = await message.reply("Starting...")

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
