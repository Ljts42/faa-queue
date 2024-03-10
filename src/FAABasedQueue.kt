import java.util.concurrent.atomic.*

/**
 * @author Sentemov Lev
 */
class FAABasedQueue<E> : Queue<E> {
    private val head : AtomicReference<Segment>
    private val tail : AtomicReference<Segment>
    private val enqIdx : AtomicLong
    private val deqIdx : AtomicLong

    init {
        val dummy = Segment(0)
        head = AtomicReference(dummy)
        tail = AtomicReference(dummy)
        enqIdx = AtomicLong(0)
        deqIdx = AtomicLong(0)
    }

    override fun enqueue(element: E) {
        do {
            val curTail = tail.get()
            val index = enqIdx.getAndIncrement().toInt()
            val segment = findSegment(curTail, index / SEGMENT_SIZE)
            tail.set(segment)
        } while (!segment.cells.compareAndSet(index % SEGMENT_SIZE, null, element))
    }

    private fun findSegment(start: Segment, id: Int) : Segment {
        var curSegment = start
        while (curSegment.id.toInt() != id) {
            curSegment.next.compareAndSet(null, Segment(curSegment.id + 1))
            curSegment = curSegment.next.get()!!
        }
        return curSegment
    }

    override fun dequeue(): E? {
        while (true) {
            if (!shouldTryToDeque()) return null
            val curHead = head.get()
            val index = deqIdx.getAndIncrement().toInt()
            val segment = findSegment(curHead, index / SEGMENT_SIZE)
            head.set(segment)
            if (!segment.cells.compareAndSet(index % SEGMENT_SIZE, null, POISONED)) {
                return segment.cells.getAndSet(index % SEGMENT_SIZE, null) as E
            }
        }
    }

    private fun shouldTryToDeque(): Boolean {
        while (true) {
            val curDeqIdx = deqIdx.get()
            val curEnqIdx = enqIdx.get()
            if (curDeqIdx == deqIdx.get()) {
                return curDeqIdx < curEnqIdx
            }
        }
    }
}

private class Segment(val id: Long) {
    val next = AtomicReference<Segment?>(null)
    val cells = AtomicReferenceArray<Any?>(SEGMENT_SIZE)
}

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2
private val POISONED = Any()
