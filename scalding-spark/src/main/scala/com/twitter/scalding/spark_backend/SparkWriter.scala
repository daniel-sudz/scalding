package com.twitter.scalding.spark_backend

import com.twitter.scalding.dagon.{HMap, Rule}
import com.twitter.scalding.typed._
import com.twitter.scalding.typed.memory_backend.AtomicBox
import com.twitter.scalding.{CFuture, Config, Execution, ExecutionCounters}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.concurrent.{ExecutionContext, Future, Promise}
import java.util.concurrent.atomic.AtomicLong

import Execution.{ToWrite, Writer}

import SparkMode.SparkConfigMethods

class SparkWriter(val sparkMode: SparkMode) extends Writer {

  private def session: SparkSession = sparkMode.session

  private val sparkCounters = new SparkCounters(session)

  private val sourceCounter: AtomicLong = new AtomicLong(0L)

  case class TempSource[A](id: Long) extends Input[A]

  object TempSource {
    def next[A](): TempSource[A] = TempSource(sourceCounter.incrementAndGet)
  }

  type StateKey[+A] = (Config, TypedPipe[A])
  type WorkVal[+A] = (Input[A], Future[RDD[_ <: A]])

  private[this] case class State(
      id: Long,
      sources: Resolver[Input, SparkSource],
      initToOpt: HMap[StateKey, TypedPipe],
      forcedPipes: HMap[StateKey, WorkVal]
  ) {

    /**
     * Returns true if we actually add this optimized pipe. We do this because we don't want to take the side
     * effect twice.
     */
    def addForce[T](
        c: Config,
        init: TypedPipe[T],
        opt: TypedPipe[T],
        rdd: Future[RDD[_ <: T]],
        persist: Option[StorageLevel]
    )(implicit ec: ExecutionContext): (State, Boolean, Future[RDD[_ <: T]]) =
      forcedPipes.get((c, opt)) match {
        case None =>
          // we have not previously forced this source
          val forcedRdd: Future[RDD[_ <: T]] =
            persist match {
              case None        => rdd
              case Some(level) => rdd.map(_.persist(level))
            }
          val ssrc: SparkSource[T] = materializedSource[T](forcedRdd)
          val src: Input[T] = TempSource.next()

          val newSources = sources.orElse(Resolver.pair(src, ssrc))
          val workVal: WorkVal[T] = (src, forcedRdd)
          val newForced = forcedPipes + ((c, opt) -> workVal)
          val newInitToOpt = initToOpt + ((c, init) -> opt)

          (copy(sources = newSources, forcedPipes = newForced, initToOpt = newInitToOpt), true, forcedRdd)
        case Some(_) =>
          (copy(initToOpt = initToOpt + ((c, init) -> opt)), false, rdd)
      }

    private def get[T](c: Config, init: TypedPipe[T]): WorkVal[T] =
      initToOpt.get((c, init)) match {
        case Some(opt) =>
          forcedPipes.get((c, opt)) match {
            case None =>
              sys.error(s"invariant violation: initToOpt mapping exists for $init, but no forcedPipe")
            case Some(wv) => wv
          }
        case None =>
          sys.error(s"invariant violation: no init existing: $init")
      }

    def getForced[T](c: Config, init: TypedPipe[T]): Future[TypedPipe[T]] =
      Future.successful(TypedPipe.from(get(c, init)._1))

    def getIterable[T](c: Config, init: TypedPipe[T])(implicit ec: ExecutionContext): Future[Iterable[T]] =
      get(c, init)._2.map { rdd =>
        // we have to convert this to a list
        // because at the end of the Execution the spark session is shutdown
        rdd.toLocalIterator.toList
      }

    // This should be called after a pipe has been forced
    def write[T](c: Config, init: TypedPipe[T], sink: Output[T])(implicit
        ec: ExecutionContext
    ): Future[Unit] =
      sparkMode.sink(sink) match {
        case None => Future.failed(new Exception(s"unknown sink: $sink when writing $init"))
        case Some(ssink) =>
          get(c, init)._2.flatMap(ssink.write(session, c, _))
      }
  }

  private[this] val state = new AtomicBox[State](State(0L, Resolver.empty, HMap.empty, HMap.empty))

  private val forcedResolver: Resolver[Input, SparkSource] =
    new Resolver[Input, SparkSource] {
      def apply[A](ts: Input[A]) =
        state.get().sources(ts)
    }

  private def materializedSource[A](persisted: Future[RDD[_ <: A]]): SparkSource[A] =
    new SparkSource[A] {
      def read(s: SparkSession, config: Config)(implicit ec: ExecutionContext): Future[RDD[_ <: A]] =
        if (session != s)
          Future.failed(
            new Exception(
              "SparkSession has changed, illegal state. You must not share TypedPipes across Execution runs"
            )
          )
        else {
          persisted
        }
    }

  def finished(): Unit =
    state.set(null)

  def getForced[T](conf: Config, initial: TypedPipe[T])(implicit
      cec: ExecutionContext
  ): Future[TypedPipe[T]] =
    state.get().getForced(conf, initial)

  def getIterable[T](conf: Config, initial: TypedPipe[T])(implicit
      cec: ExecutionContext
  ): Future[Iterable[T]] =
    state.get().getIterable(conf, initial)

  def start(): Unit = ()

  /**
   * do a batch of writes, possibly optimizing, and return a new unique Long.
   *
   * empty writes are legitimate and should still return a Long
   */
  def execute(conf: Config, writes: List[ToWrite[_]])(implicit
      cec: ExecutionContext
  ): CFuture[(Long, ExecutionCounters)] = {

    val planner =
      SparkPlanner.plan(
        conf,
        sparkMode.sources.orElse(state.get().sources),
        sparkCounters.accumulator
      )

    import Execution.ToWrite._

    val phases: Seq[Rule[TypedPipe]] =
      OptimizationRules.standardMapReduceRules // probably want to tweak this

    val optimizedWrites = ToWrite.optimizeWriteBatch(writes, phases)

    type Action = () => Future[Unit]
    val emptyAction: Action = () => Future.successful(())

    def force[T](opt: TypedPipe[T], keyPipe: TypedPipe[T], oldState: State): (State, Action) = {
      val promise = Promise[RDD[_ <: T]]()
      val (newState, added, forcedRDD) = oldState.addForce[T](
        conf,
        init = keyPipe,
        opt = opt,
        promise.future,
        conf.getForceToDiskPersistMode.orElse(Some(StorageLevel.DISK_ONLY))
      )
      def action = () => {

        /**
         * actually run
         *
         * count is a hack to force a synchronous evaluation of the RDD. There is currently only a synchronous
         * API for un-persist but not for persist. This strategy have been used even inside spark sql itself:
         * https://github.com/apache/spark/pull/30403/files#diff-aa467582590608ec702c1caf6208e79af5e6f3ee2cfd58a53bdf2601cd5015d6R65
         * so it's not too crazy, but it does have a performance overhead that we will want to address in the
         * future.
         */
        val op = planner(opt)
        val rddF = op
          .run(session)
        promise.completeWith(rddF)
        forcedRDD.map { x =>
          x.count // any cheap RDD action to force DAG execution works here
          ()
        }
      }
      (newState, if (added) action else emptyAction)
    }
    def write[T](
        opt: TypedPipe[T],
        keyPipe: TypedPipe[T],
        sink: Output[T],
        oldState: State
    ): (State, Action) = {
      val promise = Promise[RDD[_ <: T]]()
      val (newState, added, _) = oldState.addForce[T](conf, init = keyPipe, opt = opt, promise.future, None)
      val action = () => {
        val rddF =
          if (added) {
            // actually run
            val op = planner(opt)
            val rddF = op.run(session)
            promise.completeWith(rddF)
            rddF.map(_ => ())
          } else Future.successful(())

        rddF.flatMap(_ => newState.write(conf, keyPipe, sink))
      }
      (newState, action)
    }

    /**
     * We keep track of the actions to avoid calling run on any RDDs until we have fully built the entire next
     * state
     */
    val (id: Long, acts) = state.update { s =>
      val (nextState, acts) = optimizedWrites.foldLeft((s, List.empty[Action])) {
        case (old @ (state, acts), OptimizedWrite(pipe, Force(opt))) =>
          val (st, a) = force(opt, pipe, state)
          (st, a :: acts)
        case (old @ (state, acts), OptimizedWrite(pipe, ToIterable(opt))) =>
          val (st, a) = force(opt, pipe, state)
          (st, a :: acts)
        case ((state, acts), OptimizedWrite(pipe, ToWrite.SimpleWrite(opt, sink))) =>
          val (st, a) = write(opt, pipe, sink, state)
          (st, a :: acts)
      }
      (nextState.copy(id = nextState.id + 1), (nextState.id, acts))
    }

    // now we run the actions
    CFuture.uncancellable(
      Future.traverse(acts)(fn => fn()).map(_ => (id, sparkCounters.asExecutionCounters))
    )
  }
}
