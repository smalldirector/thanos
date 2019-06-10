---
title: Contribute to docs
type: docs
menu: contributing
---

# How to contribute to Docs/Website

`./docs` directory is used as markdown source files using [blackfriday](https://github.com/russross/blackfriday) to render Thanos website resources.

However the aim for those is to also have those `*.md` files renderable and usable (including links) via GitHub.

To make that happen we use following rules and helpers that are listed here

## Front Matter

[Front Matter](https://gohugo.io/content-management/front-matter/) is essential on top of every markdown file if 
you want to link this file into any menu/submenu option. We use YAML formatting. This will render
in GitHub as markdown just fine:

```md

---
title: <titke>
type: ...
weight: <weight>
menu: <where to link files in>  # This also is refered in permalinks.
---

```

## Links

Aim is to match linking behavior in website being THE SAME as Github. This means:

* For files in Hugo <content> dir (so `./docs`). Put `slug: /<filename with extension>`
* For any sub dir add to `website/hugo.yaml` new dir as key  `permalinks:` with `<dir>: /<dir>/:filname.md`

Then everywhere use native markdown *relative* symbolic links if you want to reference some mardkown file from `docs`:

`[title]( relative path to .md file )`

Or absolute path to the project repository if you want to link to exact commit e.g:

`[title]( /Makefile )`

Small [post processing script](/scripts/websitepreprocess.sh) adjusts link for Hugo rendering.

NOTE: Spaces matters so: `[xxx]( link` and `[xxx] (link` will not work.

Why?

* Links works on GitHub
* Links works on website
* Markdown plugins works as expected (e.g IDE integrations)
* We use liche to test links.

## Sections/Menu

New menus `.Site.Menus` are added as soon as some file has Front Matter with certain `menu`.

Keep `menu` the same as sub-directory the file is in. This will help to manage all docs.

Show new menu section in main page by changing `website/layouts/_default/baseof.html` file.

## Logos

If your company is using Thanos in production, we'd love to showcase your company's logo on our page. Add yourself in [`website/data/sponsors.yml`](/website/data/sponsors.yml) like so:

```yml
- name: My Awesome Company
  url: https://wwww.company.com
  logo: company.png
```

Copy your company's logo in `website/static/logos`, and make sure it follows these rules:

* Rectangle shape
* Greyscale is prefered but color is fine
* Keep it under 50KB

## Testing

Every PR is building website and on success it shows the link to preview.

## Deployment.

We use [Netlify](https://www.netlify.com/) for hosting. We are using Open Source license (PRO). Thanks Netlify for this!

On every commit to `master` netlify runs CI that invokes `make web` (defined in [netlify.toml](https://github.com/improbable-eng/thanos/blob/master/netlify.toml)))

NOTE: Check for status badge in README for build status on the page.

If master build for netlify succeed, the new content is published automatically.