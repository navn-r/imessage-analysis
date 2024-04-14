## iMessage Analysis

### Objectives

- Parse `chat.db` to extract conversation, store it in a DataFrame
- Analyze metrics from the conversation with visualizations
- Answer selected questions about the conversation
- ~~Connect LlamaIndex and LLM model with data to further answer questions~~
- Redact all personal information (including personal conversation specific analysis) and publish results

**Related**:

- https://simon-aubury.medium.com/my-data-your-llm-paranoid-analysis-of-imessage-chats-with-openai-llamaindex-duckdb-60e5eb9e23e3
- https://github.com/saubury/paranoid_text_LLM/
- https://blog.langchain.dev/tutorial-chatgpt-over-your-data/

### Data

- Single conversation, jumps between iMessage and Text
  - Start Date: January 28th 2023
  - End Date: March 31st 2024
- Need to handle reply threads and reaction messages: `Loved "original message"`
  - Avoid double counting messages
- Ignore non-text messages, but keep track? `messages.type == 'Attachment'`
- Preserve emojis, unsure about stickers
- Stop word filtering? Need to preserve capitalization due to tone and sentiment

**Options**:

1. Self made SQL query, then drop into DataFrame 
   - Lighter weight due to a single conversation
   - Can build off of existing SQL queries, and trim it down

2. Use existing iMessage extraction library
   - Less investigation, more plug and play
   - Have to stick with how the library handles column names
   - Has exporting features built in, but not needed

**Chosen**: Self made SQL query (See [`imessage.py`](imessage.py))

### Metrics, Visualizations, Questions

#### Message Count

- Total vs per person
- Averages, peaks, valleys
- Longest streak of days talked, did we miss a day?
- Which day of the week is the most active? Least active?
- Monthly, weekly, daily trends
- Favorite active hour of the day? What time am I most active versus them?
- Time of day?
- Ratio of messages sent vs received, average vs over time
- What time are these ratios the highest? The lowest?
- Ratio of text to attachment based messages
- Longest thread of messages, based on number of replies
- Ratio of emoji-only messages to full text messages

#### Word Count

- Average message length, longest message per person
- Longest thread of messages, based on word count
- Frequency of specific words and phrases
- Emoji count, top emojis used
  - What time of day are emojis used the most? Is there a trend?


Made with ❤️ in 🍁
