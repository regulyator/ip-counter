package org.example

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock

const val MID_IP = 0x7FFFFFFF
const val MID_OFFSET = 0x80000000

//here we can set the batch size and channel capacity to the values that are optimal for the system
const val BATCH_SIZE = 1500
const val CHANNEL_CAPACITY = 1600
const val CONSUMER_DISPATCHER_MULTIPLIER = 1.2


fun main(args: Array<String>) = runBlocking {
    println("Starting processing...")
    val timeStart = System.currentTimeMillis()
    val scannedFilePath = validateAndGetScannedPath(args)
    val ipCounterHolder = IpCounterHolder()

    val systemProcessors = (Runtime.getRuntime().availableProcessors() * CONSUMER_DISPATCHER_MULTIPLIER).toInt()
    val consumersExecutor = Executors.newFixedThreadPool(systemProcessors)
    val consumersDispatcher = consumersExecutor.asCoroutineDispatcher()
    val producerDispatcher = Dispatchers.IO
    val linesChannel = Channel<Array<String>>(CHANNEL_CAPACITY)

    val coroutineExceptionHandler = CoroutineExceptionHandler { _, exception ->
        println("Error: ${exception.message}")
    }

    try {
        val producer = startProducers(producerDispatcher, scannedFilePath, linesChannel, coroutineExceptionHandler)
        val consumers = startConsumers(
            systemProcessors,
            consumersDispatcher,
            linesChannel,
            ipCounterHolder,
            coroutineExceptionHandler
        )

        producer.join()
        consumers.forEach { it.join() }

        val currentTime = System.currentTimeMillis()
        println("Processing finished in ${(currentTime - timeStart) / 1000} seconds")
        println("Unique IPs count: ${ipCounterHolder.getUniqueIpsCount()}")
        consumersExecutor.shutdown()
    } catch (e: Exception) {
        println("Error: ${e.message}")
        consumersExecutor.shutdown()
    }
}

private fun validateAndGetScannedPath(args: Array<String>): String {
    require(args.size == 1) { "Please provide a path to the file with scanned IP addresses" }
    val scannedFilePath = args[0]
    require(File(scannedFilePath).exists()) { "File with scanned IP addresses not found" }
    return scannedFilePath
}

//channel producer, that reads the file and sends batches of lines to the channel
private fun CoroutineScope.startProducers(
    producerDispatcher: CoroutineDispatcher,
    scannedFilePath: String,
    linesChannel: Channel<Array<String>>,
    coroutineExceptionHandler: CoroutineExceptionHandler
) = launch(producerDispatcher + coroutineExceptionHandler) {
    BufferedReader(FileReader(scannedFilePath)).use { reader ->
        val buffer = Array(BATCH_SIZE) { "" }
        var bufferIdx = 0
        var line: String? = reader.readLine()
        while (line != null) {
            buffer[bufferIdx] = line
            if (bufferIdx == BATCH_SIZE - 1) {
                bufferIdx = 0
                linesChannel.send(buffer.copyOfRange(0, BATCH_SIZE))
            } else {
                bufferIdx++
            }
            line = reader.readLine()
        }
        if (bufferIdx > 0) {
            linesChannel.send(buffer.copyOfRange(0, bufferIdx))
        }
    }
    linesChannel.close()
}

// simple channel consumer, that adds IPs to the counter
private fun CoroutineScope.startConsumers(
    systemProcessors: Int,
    consumersDispatcher: ExecutorCoroutineDispatcher,
    linesChannel: Channel<Array<String>>,
    ipCounterHolder: IpCounterHolder,
    coroutineExceptionHandler: CoroutineExceptionHandler
) = List(systemProcessors) {
    launch(consumersDispatcher + coroutineExceptionHandler) {
        for (batch in linesChannel) {
            ipCounterHolder.addIp(batch)
        }
    }
}

class IpCounterHolder {
    companion object {
        /*
        BitSet is the best memory efficient solution that I found, in worst case it will take 2^32 bits = 512MB of memory
        I had to split the counter into two parts to avoid problem related to fact that we don't have unsigned int
        and BitSet can't store long size, also we can use two locks to avoid contention
         */
        private val ipCounterFirstHalf = BitSet(Int.MAX_VALUE)
        private val ipCounterSecondHalf = BitSet(Int.MAX_VALUE)
        private val lockFirstHalf = ReentrantLock()
        private val lockSecondHalf = ReentrantLock()
    }

    //locking for batches shows better performance than locking for each IP, even with memory overhead
    fun addIp(ips: Array<String>) {
        val ipsFirstHalfTmp = mutableListOf<Int>()
        val ipsSecondHalfTmp = mutableListOf<Int>()
        ips.forEach {
            val ipNumeric = it.ipToLong()
            if (ipNumeric == -1L) return@forEach
            if (ipNumeric <= MID_IP) {
                ipsFirstHalfTmp.add(ipNumeric.toInt())
            } else {
                ipsSecondHalfTmp.add((ipNumeric - MID_OFFSET).toInt())
            }
        }


        try {
            lockFirstHalf.lock()
            ipsFirstHalfTmp.forEach { ipCounterFirstHalf.set(it) }
        } finally {
            lockFirstHalf.unlock()
        }

        try {
            lockSecondHalf.lock()
            ipsSecondHalfTmp.forEach { ipCounterSecondHalf.set(it) }
        } finally {
            lockSecondHalf.unlock()
        }

    }

    fun getUniqueIpsCount(): Int {
        return ipCounterFirstHalf.cardinality() + ipCounterSecondHalf.cardinality()
    }
}

// utils methods for parsing and validating IP addresses (I know that there are libraries for that, but I relied on the profiler results)
private fun String.ipToLong(): Long {
    val parts = this.splitIpString()
    if (parts.size != 4) return -1L
    var result = 0L
    (3 downTo 0).forEach {
        try {
            val part = parts[3 - it]
            result = result or (part shl (it * 8))
        } catch (e: NumberFormatException) {
            return -1L
        }
    }
    return result
}

private fun String.splitIpString(): LongArray {
    val result = LongArray(4)
    var delimeter = 0
    var resultPointer = 0
    for (idx in this.indices) {
        if (this[idx] == '.') {
            val part = this.parsePart(delimeter, idx)
            if (part !in 0..255) return LongArray(0)
            result[resultPointer++] = part
            delimeter = idx + 1
        }
    }

    val part = this.parsePart(delimeter, this.length)
    if (part !in 0..255) return LongArray(0)
    result[resultPointer] = part
    return result
}

private fun String.parsePart(start: Int, end: Int): Long {
    var resultPart: Long = 0
    for (i in start until end) {
        val char = this[i]
        if ((char < '0') or (char > '9')) {
            return -1
        }
        resultPart = resultPart * 10 + (char - '0')
    }
    return resultPart
}
