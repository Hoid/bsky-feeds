import { QueryParams } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { AppContext } from '../config'
import { AlgoManager } from '../addn/algoManager'
import dotenv from 'dotenv'
import { Post } from '../db/schema'
import dbClient from '../db/dbClient'
import { PostView } from '@atproto/api/dist/client/types/app/bsky/feed/defs'
import { AppBskyEmbedRecord } from '@atproto/api'
import { Record } from '../lexicon/types/app/bsky/feed/post'

dotenv.config()

// max 15 chars
export const shortname = 'ratiod'

export const handler = async (ctx: AppContext, params: QueryParams) => {
  const builder = await dbClient.getLatestPostsForTag(
    shortname,
    params.limit,
    params.cursor,
  )

  const feed = builder.map((row) => {
    let lookup_uri = row.uri

    if (row.embed?.record?.uri) {
      lookup_uri = row.embed.record.uri
    } else {
      if (row.replyParent) lookup_uri = row.replyParent
      else return false
    }
    return { post: lookup_uri }
  })

  let cursor: string | undefined
  const last = builder.at(-1)
  if (last) {
    cursor = `${new Date(last.indexedAt).getTime()}::${last.cid}`
  }

  return {
    cursor,
    feed,
  }
}

export class manager extends AlgoManager {
  public name: string = shortname

  public threshold = 5 // only requires 5 replies to be counted

  public static cacheAge(params): Number {
    return 60
  }

  public async periodicTask() {
    await this.db.removeTagFromOldPosts(
      this.name,
      new Date().getTime() - 24 * 60 * 60 * 1000, // one day ago
    )
    await dbClient.aggregatePostsByRepliesToCollection(
      'post',
      shortname,
      this.threshold,
      'ratiod_posts',
      100
    )

    const ratiod_posts_and_replies = await dbClient.getCollection('ratiod_posts')

    let updated = 0

    console.log(`${this.name}: ${ratiod_posts_and_replies.length} posts updating...`)

    for (let i = 0; i < ratiod_posts_and_replies.length; i++) {
      let likes: number = Number.isInteger(ratiod_posts_and_replies[i].likes)
        ? ratiod_posts_and_replies[i].likes
        : 0
      let replies: number = Number.isInteger(ratiod_posts_and_replies[i].replies)
        ? ratiod_posts_and_replies[i].replies
        : 0
      let reposts: number = Number.isInteger(ratiod_posts_and_replies[i].reposts)
        ? ratiod_posts_and_replies[i].reposts
        : 0
      let quotes: number = Number.isInteger(ratiod_posts_and_replies[i].quotes)
        ? ratiod_posts_and_replies[i].quotes
        : 0

      var quotedPostUri: string | null = null
      var postText: string = ""
      try {
        const rootPostThread = await this.agent.getPostThread({
          uri: ratiod_posts_and_replies[i]._id.toString(),
          depth: 1,
        })
        const rootPost = <PostView>rootPostThread.data.thread.post
        const postRecord = rootPost.record as Record
        postText = postRecord.text
        if (rootPost.likeCount) {
          likes = rootPost.likeCount!
        } else {
          likes = 0
        }
        if (rootPost.replyCount) {
          replies = rootPost.replyCount!
        } else {
          replies = 0
        }
        if (rootPost.repostCount) {
          reposts = rootPost.repostCount!
        } else {
          reposts = 0
        }
        // const quotePostsArray = await dbClient.countQuotePosts(rootPost.uri, shortname)
        // if (quotePostsArray) {
        //   console.log(`quotePostsArray: ${quotePostsArray}`)
        //   quotes = quotePostsArray['quotes_count']
        //   console.log(`Found ${quotes} quote posts for post ${rootPost.uri}`)
        // } else {
        quotes = 0
        // }
        const embed = rootPost.embed as AppBskyEmbedRecord.Main
        const linkedUri = embed?.record?.uri
        if (linkedUri
          && linkedUri.toLowerCase().includes("bsky.app")
          && linkedUri.toLowerCase().includes("/post/")) {
          quotedPostUri = linkedUri
        }
      } catch (err) {
        console.log(
          `${this.name}: Cannot retrieve ${ratiod_posts_and_replies[i]._id.toString()}`,
        )
        likes = 0
        replies = 0
        reposts = 0
        quotes = 0
        continue
      }

      const record = {
        _id: ratiod_posts_and_replies[i]._id,
        indexedAt: ratiod_posts_and_replies[i].indexedAt,
        likes: likes,
        replies: replies,
        reposts: reposts,
        quotes: quotes,
        quotedPostUri: quotedPostUri,
        sort_weight:
          this.ratiodAlgorithm(postText, replies, quotes, likes, reposts) ? (replies + quotes / ((likes + reposts) * 0.85)) : 0
      }

      updated++
      await dbClient.insertOrReplaceRecord(
        { _id: record._id },
        record,
        'ratiod_posts',
      )
    }

    console.log(
      `${this.name}: ${ratiod_posts_and_replies.length} updated (${updated} from server)`,
    )
  }

  private ratiodAlgorithm(postText: string, replies: number, quotes: number, likes: number, reposts: number) {
    if (postText.toLowerCase().includes("reply with")
      || postText.toLowerCase().includes("respond with")
      || postText.toLowerCase().includes("?")
      || postText.toLowerCase().includes("q&a")
    ) {
      return false
    }
    const controversialInteractions = replies + quotes
    const positiveInteractions = likes + reposts
    const isControversial = controversialInteractions > 10
    const repostThreshold = reposts * 10
    const isRatiod = controversialInteractions > (positiveInteractions * 0.85) && controversialInteractions > repostThreshold
    return isControversial && isRatiod
  }

  public async filter_post(post: Post): Promise<Boolean> {
    return post.replyRoot !== null && post.replyRoot.split('/')[2] != post.author
  }
}
