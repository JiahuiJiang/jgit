package org.eclipse.jgit.internal.storage.dfs;

import org.eclipse.jgit.internal.JGitText;

// TODO: switch to Immutable when we move it internal and can add more external dependencies
public class DfsBlockCaffeineCacheConfig {

    private final int blockSize;
    private final long cacheMaximumSize;
    private final int packFileExpireSeconds;
    private final double streamRatio;

    public DfsBlockCaffeineCacheConfig(
            int blockSize, long cacheMaximumSize, int packFileExpireSeconds, double streamRatio) {
        checkValues(blockSize);
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

    private void checkValues(int blockSize) {
        if (blockSize < 512 || (blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException("blockSize must be a power of 2 and at least 512.");
        }
    }

}
