<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/banner-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/banner-light.svg">
    <img alt="arx icon of cube"
         src="docs/assets/icon.svg"
         width="70%">
    </picture>
</div>

# arx

This project aims to create a service, commandline, package/archive format allowing for the creation and distribution of "Artifact". An Artifact is any kind of file structure with some metadata attached to it. It could represent anything, as long as it can be defined through a file hirachy.

## Design

The ArtifactRepository design is based on a Sha-512 Merkel Tree which serves as the foundation of the Artifact. Each file within an Artifact is content addressed by its sha512 hash and directories are stored in an identical format to git. In general the object structure closely matches that of git. 

The very top level of an Artifact is called an **Index** (git calls it a commit). This defines the artifact and contains any and all relevant metadata, such as the timestamp of creation. But since it simply contains any and all metadata in a simple key value format similar to HTTP headers, it allows for the same level of flexibility and metadata to be attached to an index.
Some possibilities could include:
* Git has of source that produced output
* Version number of produced artifact
* Deployment configuration (Debug vs Release)
* GPG Signature
* Artifact type

One required key however is the `tree` key which defines the hash of the top level tree which forms the root of the artifact. This tree can be iterated over to discover more trees and blobs which together make up the entirety of the artifact.

Trees & Blobs are directly inherited from Git's design and would be interoperable if it weren't for the differing hash sizes.

## Benefits

One of the key benefits of going with a merkel tree approach very similar to git is the automatic deduplication which occurs. Since Artifacts are stored as individual files indexed by their content on the server, One file contained within multiple artifacts (multiple copies of a library) is deduplicated amongst them all and only 1 copy is stored. This also works perfectly for horizontally scaling multiple servers which can operate on the same exact data store without running into conflicts (deletion can still cause problems but that is not the primary focus).

This deduplication extends all the way into the Artifact archive format which thanks to the hashes will also only store a single copy of each file. This makes for an incredibly efficient archive format when many duplicate files are to be expected.

## Client

The AR Client is a small commandline utility allowing for local creation of Artifacts, Either in the .ar archive format or as a local artifact store similar to the one in the server (useful when creating multiple artifacts on the same machine and deduplication is desired)

It supports Uploading and Downloading Indexes to/from the ArtifactRepository Server which allows for distribution of artifacts amongst clients via the index hash.

## Server

The ArtifactRepository Server is a simple HTTP server which with basic REST calls allows uploading & downloading artifacts. Internally it stores these in its content addressed store which allows for multiple servers to back onto the same data source, enabling horizontal scaling.

One of the key functionalities of a server is that it allows for exactly one upstream to defined which makes the server act in a sort of relay mode. Any artifacts uploaded to it will be mirrored to the upstream, and any artifacts requested will be queried against the upstream if they are not present locally. This allows for multi-tiered caching through the use of machine local, region local and global instances which mirror data between them depending on where the data is required.

Another key consideration is the ability to back a sever onto local file systems as well as S3 compatible object storage API's for global replication and high availability.
## Artifact File Format

The artifact file format `.ar` is an Archive format which is purpose built for artifacts.

It is structured as follows:

| Data    | Description                         |
| ------- | ----------------------------------- |
| [u8; 4] | header / magic number               |
| [u8; 2] | compression method                  |
| [u8; N] | data (compressed with above method) |

With the data layout being as follows

| Section       | Description |
| ------------- | ------------- |
| [HEADER]      | Archive header |
| [INDEX]       | Index file |
| [BLOBS/TREES] | Collection of Blobs & Trees |

The HEADER must be laid out as follows:
| Data | Description |
| ---- | ----------- |
| [entry; N] | Entries |
| [u8; 1] | Null Terminator |

with each entry as follows:

| Data    | Description |
| ------- | ----------- |
| [u8;64] | Hash        |
| [u64]   | Offset      |
| [u64]   | Length      |

All data within the data should be stored in its uncompressed form and taken directly from the binary object records.

A supplementary artifact format `.sar` is entirely identical but without the requirement for every blob/tree to be present. Only those within the HEADER are guaranteed to exist within the archive and as such can aid in cutting down on data transmitted when a server/client is only missing a small number of files.
