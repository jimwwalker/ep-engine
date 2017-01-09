#include "collections/vbucket_manifest_entry.h"

std::ostream& Collections::VBucket::operator<<(
        std::ostream& os,
        const Collections::VBucket::ManifestEntry& manifestEntry) {
    os << "ManifestEntry: collection:" << *manifestEntry.collectionName
       << ", revision:" << manifestEntry.revision
       << ", start_seqno:" << manifestEntry.start_seqno
       << ", end_seqno:" << manifestEntry.end_seqno;
    return os;
}