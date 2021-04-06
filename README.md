## A Spanish reddit dialogues corpus

We collect comments from Reddit during 2019. We first download all the comments submitted in 2019 from the [pushshift.io](https://files.pushshift.io/reddit/); then we identify the language of each comment using a classifier trained by fastText; finally, we match comments based on their parent_id to construct the context. Our Spanish Reddit dialogue corpus contains 2,012,992 conversations.

