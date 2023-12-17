import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'
import dotenv from 'dotenv'

import algos from './algos'
import batchUpdate from './addn/batchUpdate'

import { Database } from './db'

import crypto from 'crypto'
import { Post } from './db/schema'
import { AppBskyEmbedExternal, AppBskyEmbedRecord, BskyAgent } from '@atproto/api'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  public algoManagers: any[]

  constructor(db: Database, subscriptionEndpoint: string) {
    super(db, subscriptionEndpoint)

    this.algoManagers = []

    const agent = new BskyAgent({ service: 'https://bsky.social' })

    dotenv.config()
    const handle = `${process.env.FEEDGEN_HANDLE}`
    const password = `${process.env.FEEDGEN_PASSWORD}`

    agent.login({ identifier: handle, password: password }).then(async () => {
      batchUpdate(agent, 5 * 60 * 1000)

      Object.keys(algos).forEach((algo) => {
        this.algoManagers.push(new algos[algo].manager(db, agent))
      })

      const startPromises = this.algoManagers.map(async (algoManager) => {
        if (await algoManager._start()) {
          console.log(`${algoManager.name}: Started`)
        }
      })

      await Promise.all(startPromises)
    })
  }

  public authorList: string[]
  public intervalId: NodeJS.Timer

  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return

    await Promise.all(this.algoManagers.map((manager) => manager.ready()))

    const ops = await (async () => {
      try {
        return await getOpsByType(evt)
      } catch (e) {
        console.log(`core: error decoding ops ${e.message}`)
        return undefined
      }
    })()

    if (!ops) return

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)

    const englishPosts = ops.posts.creates.filter((post) => {
      return post.record.langs?.length === 1 && post.record.langs?.includes("en")
    })

    // Transform posts in parallel
    const postsCreated = englishPosts.map((post) => {
      return {
        _id: null,
        uri: post.uri,
        cid: post.cid,
        author: post.author,
        text: post.record?.text,
        replyParent: post.record?.reply?.parent.uri ?? null,
        replyRoot: post.record?.reply?.root.uri ?? null,
        indexedAt: new Date().getTime(),
        algoTags: null,
        embed: post.record?.embed,
        tags: Array.isArray(post.record?.tags) ? post.record?.tags : [],
      }
    })

    const postsToCreatePromises = postsCreated.map(async (post) => {
      const algoTagsPromises = this.algoManagers.map(async (manager) => {
        try {
          const includeAlgo = await manager.filter_post(post)
          return includeAlgo ? manager.name : null
        } catch (err) {
          console.error(`${manager.name}: filter failed`, err)
          return null
        }
      })

      const algoTagsResults = await Promise.all(algoTagsPromises)
      const algoTags = algoTagsResults.filter((tag) => tag !== null)

      if (algoTags.length === 0) return null

      const hash = crypto
        .createHash('shake256', { outputLength: 12 })
        .update(post.uri)
        .digest('hex')
        .toString()

      return {
        ...post,
        _id: hash,
        algoTags: algoTags,
      }
    })

    const postsToCreate = (await Promise.all(postsToCreatePromises)).filter(
      (post) => post !== null,
    )

    if (postsToDelete.length > 0) {
      await this.db.deleteManyURI('post', postsToDelete)
    }

    if (postsToCreate.length > 0) {
      postsToCreate.forEach(async (postToInsert) => {
        if (postToInsert)
          await this.db.replaceOneURI('post', postToInsert.uri, postToInsert)
      })
    }
  }
}
