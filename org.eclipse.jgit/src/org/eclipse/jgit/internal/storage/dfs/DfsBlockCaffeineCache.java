package org.eclipse.jgit.internal.storage.dfs;

import com.github.benmanes.caffeine.cache.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public final class DfsBlockCaffeineCache extends DfsBlockCache {

    private static final Logger log = LoggerFactory.getLogger(DfsBlockCaffeineCache.class);

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
    private final Cache<DfsPackDescription, DfsPackFile> packFileCache;

    /** Cache of Dfs blocks. */
    private final Cache<DfsPackKeyWithPosition, Ref> dfsBlockCache;

    public static void reconfigure(final DfsBlockCaffeineCacheConfig cacheConfig) {
        DfsBlockCache.setInstance(new DfsBlockCaffeineCache(cacheConfig));
    }

    private DfsBlockCaffeineCache(DfsBlockCaffeineCacheConfig cacheConfig) {
        maxStreamThroughCache = (long) (cacheConfig.getCacheMaximumSize() * cacheConfig.getStreamRatio());

        blockSize = cacheConfig.getBlockSize();

        // TODO: register the cache and expose the stats
        // the estimated retained bytes is estimated based on the fields each object contains
        packFileCache = Caffeine.newBuilder()
                .removalListener((DfsPackDescription description, DfsPackFile packFile, RemovalCause cause) -> {
                    if (packFile != null) {
                        log.debug("PackFile {} is removed because it {}", packFile.getPackName(), cause);
                        packFile.key.cachedSize.set(0);
                        packFile.close();
                    }})
                .maximumWeight(cacheConfig.getCacheMaximumSize() / 2)
                .weigher((DfsPackDescription description, DfsPackFile packFile) -> {
                    // weight is static after creation and update, so here we are relying on dfsBlockCache's removal
                    // listen to make sure the retained size of packFile won't exceed the given memory
                    long estimatedSize = 2048 + blockSize;
                    return estimatedSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) estimatedSize;
                })
                .recordStats()
                .build();

        dfsBlockCache = Caffeine.newBuilder()
                .removalListener((DfsPackKeyWithPosition keyWithPosition, Ref ref, RemovalCause cause) -> ref = null)
                .maximumWeight(cacheConfig.getCacheMaximumSize() / 2)
                .weigher((DfsPackKeyWithPosition keyWithPosition, Ref ref) -> ref == null? 48 : 48 + ref.getSize())
                .recordStats()
                .build();
    }

    boolean shouldCopyThroughCache(long length) {
        return length <= maxStreamThroughCache;
    }

    DfsPackFile getOrCreate(DfsPackDescription description, DfsPackKey key) {
        DfsPackFile packFile = packFileCache.getIfPresent(description);
        if (packFile != null && !packFile.invalid()) {
            return packFile;
        }

        DfsPackFile newPackFile = new DfsPackFile(this, description, key != null ? key : new DfsPackKey());
        packFileCache.put(description, newPackFile);
        return getOrCreate(description, key);
    }

    int getBlockSize() {
        return blockSize;
    }

    DfsBlock getOrLoad(DfsPackFile pack, long position, DfsReader dfsReader) throws IOException {
        final long requestedPosition = position;
        position = pack.alignToBlock(position);
        DfsPackKey key = pack.key;

        Ref<DfsBlock> loadedBlockRef = dfsBlockCache.get(new DfsPackKeyWithPosition(key, position), keyWithPosition -> {
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

        return getOrLoad(pack, requestedPosition, dfsReader);
    }

    void put(DfsBlock block) {
        put(block.pack, block.start, block.size(), block);
    }

    public <T> Ref<T> put(DfsPackKey key, long position, int size, T value) {
        return dfsBlockCache.get(new DfsPackKeyWithPosition(key, position), keyWithPosition ->
                new Ref(keyWithPosition.getDfsPackKey(), keyWithPosition.getPosition(), size, value));
    }

    boolean contains(DfsPackKey key, long position) {
        return get(key, position) != null;
    }

    @SuppressWarnings("unchecked")
    <T> T get(DfsPackKey key, long position) {
        Ref<T> blockCache = dfsBlockCache.getIfPresent(new DfsPackKeyWithPosition(key, position));
        return blockCache != null ? blockCache.get() : null;
    }

    void remove(DfsPackFile pack) {
        packFileCache.invalidate(pack.getPackDescription());
    }

    void cleanUp() {
        packFileCache.invalidateAll();
        dfsBlockCache.invalidateAll();
    }

    private static final class DfsPackKeyWithPosition {
        private DfsPackKey dfsPackKey;
        private long position;

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
