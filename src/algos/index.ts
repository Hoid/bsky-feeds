import { AppContext } from '../config'
import {
  QueryParams,
  OutputSchema as AlgoOutput,
} from '../lexicon/types/app/bsky/feed/getFeedSkeleton'

import * as discourse from './discourse'
import * as ratiod from './ratiod'


type AlgoHandler = (ctx: AppContext, params: QueryParams) => Promise<AlgoOutput>

const algos = {
  [discourse.shortname]: {
    handler: <AlgoHandler>discourse.handler,
    manager: discourse.manager,
  },
  [ratiod.shortname]: {
    handler: <AlgoHandler>ratiod.handler,
    manager: ratiod.manager,
  },
}

export default algos
