import sqlite3
import pandas as pd
from functools import reduce


#  ____        _          ____
# |  _ \  __ _| |_ __ _  |  _ \ _ __ ___ _ __
# | | | |/ _` | __/ _` | | |_) | '__/ _ \ '_ \
# | |_| | (_| | || (_| | |  __/| | |  __/ |_) |
# |____/ \__,_|\__\__,_| |_|   |_|  \___| .__/
#                                       |_|


def drop_duplicates(df: pd.DataFrame):
    """
    When both the iMessage and the text message were delivered, their `guid` and `date`
    are identical. This function Filters duplicate `guid` rows where the iMessage
    row is kept.
    """
    duplicate_guids = df[df["guid"].duplicated() & (df["is_sms"] == 1)].index
    return df.drop(duplicate_guids)


def update_reactions(df: pd.DataFrame):
    """
    The `reaction_guid` column contains the primary key of the message that was reacted to.
    This function removes that row, and adds a column to the original message asserting that
    it was reacted to with type `reaction_type`. See observation on removed reactions.

    --- Values of `reaction_type` ---
    0: No reaction
    2: Game over (won or lost)
    3: Game in progress (start or in the middle)
    1000: Sticker
    2000-2005: Loved, Liked, Disliked, Laughed, Emphasized, Questioned
    3000-3005: Removed

    --- Observations ---
    1. If a reaction_type is removed (3xxx), the original reaction (2xxx) is absent, so we
       can just ignore those rows entirely.
    2. `guid` for text messages do not match the `reaction_guid` of their corresponding reaction.
    3. Games are rows where their `guid` is the same as their `reaction_guid`
    4. Rows have `reaction_guids` in the form of `bp:0/` or `p:[0-5]/`, not sure what they signify
        - bp:0/ reaction to link?
        - p:0/
        - p:1/ reaction to caption of image?

    --- Key Modifications ---
    1. All self-reactions are removed, there's only a couple of them and they were probably mistakes.
    2. Only the main 6 imessage reactions are kept, all other reactions are ignored (no texts, games, stickers)
    """
    reactions = df[df["reaction_guid"].notna()][
        ["guid", "reaction_guid", "reaction_type"]
    ]
    to_remove = reactions["guid"]

    # Only I reacted to my own messages a couple times, the other person didn't make this 'mistake'
    self_reactions = df[
        (
            df["guid"].isin(
                df[(df["reaction_type"] > 1000) & (df["is_from_me"] == 1)][
                    "reaction_guid"
                ]
            )
        )
        & (df["is_from_me"] == 1)
    ]

    out_reactions = reactions[
        (reactions["reaction_type"] >= 2000)
        & (reactions["reaction_type"] < 3000)
        & ~reactions["reaction_guid"].isin(self_reactions["guid"])
    ]

    # un-linkable rows
    a = df[df["guid"].isin(out_reactions["guid"])]
    b = df[df["guid"].isin(out_reactions["reaction_guid"])]
    text_message_reactions = a[~a["reaction_guid"].isin(b["guid"])]

    main_reactions = out_reactions[
        ~out_reactions["guid"].isin(text_message_reactions["guid"])
    ][["reaction_guid", "reaction_type"]]

    # apply the reaction_type to the original message row
    df.loc[df["guid"].isin(main_reactions["reaction_guid"]), "reaction_type"] = (
        main_reactions["reaction_type"].values
    )

    # remove the reaction rows, they are now redundant
    return df[~df["guid"].isin(to_remove)]


def update_text(df: pd.DataFrame):
    def parse_body(row):
        """
        The attributed_body column contains typedstream binary data that can contain the text
        of the message if `text` is empty.

        See https://github.com/my-other-github-account/imessage_tools
        """
        msg_text = row["text"]
        msg_attributed_body = row["body"]

        if msg_text:
            return msg_text.strip()

        try:
            msg_attributed_body = msg_attributed_body.decode("utf-8", errors="replace")
        except AttributeError:
            pass

        if "NSNumber" in str(msg_attributed_body):
            msg_attributed_body = str(msg_attributed_body).split("NSNumber")[0]
            if "NSString" in msg_attributed_body:
                msg_attributed_body = str(msg_attributed_body).split("NSString")[1]
                if "NSDictionary" in msg_attributed_body:
                    msg_attributed_body = str(msg_attributed_body).split(
                        "NSDictionary"
                    )[0]
                    msg_attributed_body = msg_attributed_body[6:-12]
                    return msg_attributed_body.strip()

    df["text"] = df.apply(parse_body, axis=1)
    return df.drop(columns=["body"])


def update_dates(df: pd.DataFrame):
    """
    The `date` column is in the form of a string, this function converts it to a datetime object.
    """
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_data(db_path, group_id, start_date, end_date):
    conn = sqlite3.connect(db_path)
    query = """
        SELECT 
            cm.message_id AS id, 
            m.guid, 
            m.thread_originator_guid AS thread_guid,
            datetime((m.date / 1000000000) + 978307200, 'unixepoch', 'localtime') AS date, 
            m.attributedBody AS body, 
            m.text, 
            m.is_from_me, 
            CASE
                WHEN c.service_name = "SMS" THEN 1
                ELSE 0
            END AS is_sms,
            REPLACE(REPLACE(m.associated_message_guid, 'p:0/', ''), 'bp:', '') AS reaction_guid, 
            m.associated_message_type AS reaction_type
        FROM 
            chat AS c
            JOIN chat_message_join AS cm ON c.ROWID = cm.chat_id
            JOIN message AS M ON m.ROWID = message_id
        WHERE 
            group_id = ?
            AND datetime((m.date / 1000000000) + 978307200, 'unixepoch', 'localtime') 
                BETWEEN datetime(?, '00:00:00') AND datetime(?, '23:59:59')
            AND m.attributedBody IS NOT NULL
            AND NOT m.is_empty
        ORDER BY date ASC;
    """
    res = pd.read_sql_query(query, conn, params=[group_id, start_date, end_date])
    prepare_steps = [drop_duplicates, update_reactions, update_text, update_dates]
    conn.close()

    return reduce(lambda df, func: func(df), prepare_steps, res)


#  _   _ _   _ _ _ _           _____                 _   _
# | | | | |_(_) (_) |_ _   _  |  ___|   _ _ __   ___| |_(_) ___  _ __  ___
# | | | | __| | | | __| | | | | |_ | | | | '_ \ / __| __| |/ _ \| '_ \/ __|
# | |_| | |_| | | | |_| |_| | |  _|| |_| | | | | (__| |_| | (_) | | | \__ \
#  \___/ \__|_|_|_|\__|\__, | |_|   \__,_|_| |_|\___|\__|_|\___/|_| |_|___/
#                      |___/


def get_thread_messages(df, thread_guid):
    """
    First message in thread does not have the thread_guid, so we need to concat it on top as well
    """
    return pd.concat(
        [df[df["guid"] == thread_guid], df[df["thread_guid"] == thread_guid]]
    )
