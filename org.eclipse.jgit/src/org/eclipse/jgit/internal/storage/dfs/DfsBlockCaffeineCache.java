package org.eclipse.jgit.internal.storage.dfs;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class DfsBlockCaffeineCache extends DfsBlockCache {

    /** Pack files smaller than this size can be copied through the cache. */
    private final long maxStreamThroughCache;

    /**
     * Suggested block size to read from pack files in bytes.
     * <p>
     * If a pack file does not have a native block size, this size will be used.
     * <p>
     * If a pack file has a native size, a whole multiple of the native size
     * will be used until it matches this size.
     * <p>
     * The value for blockSize must be a power of 2 and no less than 512.
     */
    private final int blockSize;

    /** Cache of pack files, indexed by description. */
    private final Map<DfsPackDescription, DfsPackFile> packFileCache;

    /** Reverse index from DfsPackKey to the DfsPackDescription. */
    private final Map<DfsPackKey, DfsPackDescription> reversePackDescriptionIndex;

    /** Cache of Dfs blocks and Indices. */
    private final Cache<DfsPackKeyWithPosition, Ref> dfsBlockAndIndicesCache;

    public static void reconfigure(final DfsBlockCaffeineCacheConfig cacheConfig) {
        DfsBlockCache.setInstance(new DfsBlockCaffeineCache(cacheConfig));
    }

    private DfsBlockCaffeineCache(DfsBlockCaffeineCacheConfig cacheConfig) {
        maxStreamThroughCache = (long) (cacheConfig.getCacheMaximumSize() * cacheConfig.getStreamRatio());

        blockSize = cacheConfig.getBlockSize();

        packFileCache = new ConcurrentHashMap<>(16, 0.75f, 1);
        reversePackDescriptionIndex = new ConcurrentHashMap<>(16, 0.75f, 1);

        dfsBlockAndIndicesCache = Caffeine.newBuilder()
                .removalListener(this::cleanUpIndices)
                .maximumWeight(cacheConfig.getCacheMaximumSize())
                // the estimated retained bytes is estimated based on the fields each object contains
                // plus the key/entry reference itself
                // DfsPackKey: {
                //   final int hash;                4 bytes
                //   final AtomicLong cachedSize;   28 bytes
                // }
                // final long position;             8 bytes
                // final int size;                  4 bytes
                // key, value reference             8 * 2 bytes
                .weigher((DfsPackKeyWithPosition keyWithPosition, Ref ref) -> ref == null? 60 : 60 + ref.getSize())
                .recordStats()
                .build();
    }

    private void cleanUpIndices(DfsPackKeyWithPosition keyWithPosition, Ref ref, RemovalCause cause) {
        if (keyWithPosition != null && ref != null) {
            DfsPackKey key = keyWithPosition.getDfsPackKey();
            long position = keyWithPosition.getPosition();

            if (position < 0) {
                // if it's an index, remove the whole packFile
                cleanUpIndicesIfExists(key);
            } else {
                // if it's not an index, decrease the cached size
                // remove the whole packFile if cachedSize is below 0
                if (key.cachedSize.addAndGet(-ref.getSize()) <= 0) {
                    cleanUpIndicesIfExists(key);
                }
            }
        }
    }

    private void cleanUpIndicesIfExists(DfsPackKey key) {
        if (key != null) {
            DfsPackDescription description = reversePackDescriptionIndex.remove(key);
            if (description != null) {
                packFileCache.remove(description);
            }
        }
    }

    boolean shouldCopyThroughCache(long length) {
        return length <= maxStreamThroughCache;
    }

    DfsPackFile getOrCreate(DfsPackDescription description, DfsPackKey key) {
        return packFileCache.compute(description, (DfsPackDescription k, DfsPackFile v) -> {
            if (v != null && !v.invalid()) {
                return v;
            }
            DfsPackKey newPackKey = key != null ? key : new DfsPackKey();
            reversePackDescriptionIndex.put(newPackKey, description);
            return new DfsPackFile(this, description, newPackKey);
        });
    }

    int getBlockSize() {
        return blockSize;
    }

    // do something when the block is invalid
    DfsBlock getOrLoad(DfsPackFile pack, long position, DfsReader dfsReader) throws IOException {
        final long requestedPosition = position;
        position = pack.alignToBlock(position);
        DfsPackKey key = pack.key;

        Ref<DfsBlock> loadedBlockRef = dfsBlockAndIndicesCache.get(new DfsPackKeyWithPosition(key, position), keyWithPosition -> {
            try {
                DfsBlock loadedBlock = pack.readOneBlock(keyWithPosition.getPosition(), dfsReader);
                key.cachedSize.getAndAdd(loadedBlock.size());
                return new Ref(keyWithPosition.getDfsPackKey(), keyWithPosition.getPosition(),
                        loadedBlock.size(), loadedBlock);
            } catch (IOException e) {
                return null;
            }
        });

        if (loadedBlockRef != null) {
            DfsBlock loadedBlock = loadedBlockRef.get();
            if (loadedBlock != null && loadedBlock.contains(key, position)) {
                return loadedBlock;
            }
        }

        // the current block is not valid, remove it and attempt `getOrLoad` again
        dfsBlockAndIndicesCache.invalidate(new DfsPackKeyWithPosition(key, position));
        return getOrLoad(pack, requestedPosition, dfsReader);
    }

    void put(DfsBlock block) {
        put(block.pack, block.start, block.size(), block);
    }

    public <T> Ref<T> put(DfsPackKey key, long position, int size, T value) {
        return dfsBlockAndIndicesCache.get(new DfsPackKeyWithPosition(key, position), keyWithPosition -> {
            // if it's not an index, increase the cached size
            if (keyWithPosition.position >= 0) {
                keyWithPosition.getDfsPackKey().cachedSize.getAndAdd(size);
            }
            return new Ref(keyWithPosition.getDfsPackKey(), keyWithPosition.getPosition(), size, value);
        });
    }

    boolean contains(DfsPackKey key, long position) {
        return get(key, position) != null;
    }

    @SuppressWarnings("unchecked")
    <T> T get(DfsPackKey key, long position) {
        Ref<T> blockCache = dfsBlockAndIndicesCache.getIfPresent(new DfsPackKeyWithPosition(key, position));
        return blockCache != null ? blockCache.get() : null;
    }

    void remove(DfsPackFile pack) {
        if (pack != null) {
            DfsPackKey key = pack.key;
            cleanUpIndicesIfExists(key);
            key.cachedSize.set(0);
        }
        // TODO: release all the blocks cached for this pack file too
        // right now those refs are not accessible anymore and will be evicted by caffeine cache eventually
    }

    void cleanUp() {
        packFileCache.clear();
        reversePackDescriptionIndex.clear();
        dfsBlockAndIndicesCache.invalidateAll();
    }

    private static final class DfsPackKeyWithPosition {
        private final DfsPackKey dfsPackKey;
        private final long position;

        public DfsPackKeyWithPosition(DfsPackKey dfsPackKey, long position) {
            this.dfsPackKey = dfsPackKey;
            this.position = position;
        }

        public DfsPackKey getDfsPackKey() {
            return dfsPackKey;
        }

        public long getPosition() {
            return position;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof DfsPackKeyWithPosition)) {
                return false;
            }
            DfsPackKeyWithPosition that = (DfsPackKeyWithPosition) other;
            return Objects.equals(this.getDfsPackKey(), that.getDfsPackKey())
                    && this.getPosition() == that.getPosition();
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.getDfsPackKey(), this.getPosition());
        }

    }

    public static final class Ref<T> extends DfsBlockCache.Ref<T> {
        final DfsPackKey key;
        final long position;
        final int size;
        volatile T value;

        Ref(DfsPackKey key, long position, int size, T v) {
            this.key = key;
            this.position = position;
            this.size = size;
            this.value = v;
        }

        T get() {
            return value;
        }

        boolean has() {
            return value != null;
        }

        int getSize() {
            return size;
        }
    }
}
