package org.eclipse.jgit.internal.storage.dfs;

public class DfsBlockCaffeineCacheConfig {

    private final int blockSize;
    private final long cacheMaximumSize;
    private final int packFileExpireSeconds;
    private final double streamRatio;

    public DfsBlockCaffeineCacheConfig(
            int blockSize, long cacheMaximumSize, int packFileExpireSeconds, double streamRatio) {
        this.blockSize = blockSize;
        this.cacheMaximumSize = cacheMaximumSize;
        this.packFileExpireSeconds = packFileExpireSeconds;
        this.streamRatio = streamRatio;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public long getCacheMaximumSize() {
        return cacheMaximumSize;
    }

    public int getPackFileExpireSeconds() {
        return packFileExpireSeconds;
    }

    public double getStreamRatio() {
        return streamRatio;
    }

}
