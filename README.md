<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/banner-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/banner-light.svg">
    <img alt="arx icon of cube"
         src="docs/assets/icon.svg"
         width="70%">
    </picture>
</div>

The home of the ARX archiving system, It includes the cli, server and documentation / designs.

---

## Why ARX?

* **Deduplicating:** ARX deduplicates all files being archived, Before they are compressed. This results in smaller archive files and no wasted bandwidth.

* **Reliable/Resilient:** Built on the same architecture as Git, ARX Provides data integritry guarantees ensuring all data Extracted is identical to the original.

* **Fast:** Utilizing high performance Rust with a simple core design ensures generating archives is always fast and efficient.

## Quick Start

// TODO: Link to Installation documentation / setup instructions

## Getting Help

ARX has a community [matrix channel](https://matrix.to/#/#arx:cerver.au) for any questions, feedback or socializing.

## Contributing

See [Contributing](CONTRIBUTING.md) Guidelines 

For a detailed explanation of the ARX design and the archive format see the [design](docs/designs/design.md)

## License

ARX is distributed under the terms of the [GPL-2.0 License](https://github.com/ArtifactRepository/arx/blob/master/LICENSE) and derives from [Git](https://github.com/git/git)s designs also licensed under GPL-2.0.
