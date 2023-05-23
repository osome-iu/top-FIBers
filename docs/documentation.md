---
title: "Updating this documentation"
last_modified: "2023-05-06"
---
> Last modified: {{ page.last_modified | date: "%Y-%m-%d"}}

The documentation on this site is rendered automatically by [GitHub Pages](https://docs.github.com/en/pages/getting-started-with-github-pages/creating-a-github-pages-site).

**Adding a new page to this documentation is as simple as creating a [markdown file](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet) (extension `.md`).**

Important things to remember:

1. **Documentation markdown files _must be created inside of the `docs/` directory in the GitHub reposity for this project_**
2. **Do your best to keep the `docs/` directory organized nicely.**
Some general organization conventions:
- Keep file names short
- Keep the directory organized based on _topics_
    - For example, should a group of files emerge that relate to a similar topic, please create a subdirectory with a name associated with that topic and save those related files within that directory.
    Then make sure to do the below:
        - Make a top-level page for that sub-directory that points to all of those related files
        - Update any paths referencing files that had already been created, as they may have been broken by reorganizing the directory
3. **Include a `title` in the "[front matter](https://docs.github.com/en/pages/setting-up-a-github-pages-site-with-jekyll/about-github-pages-and-jekyll#front-matter)" to create a title that formats nicely.**
- To do this simply include the below code at the very top of your markdown file:
    ```
    ---
    title: "The title for your documentation page goes here"
    ---
    ```
- Note: The quotes are needed in the title above
4. **Include the date when you created/updated that documentation page**
Include the `last_modified` variable in the front matter with the date you update the file.
Then, right under it, include a "Last modified" note.
It should look like the below...
```
---
title: "The title for your documentation page goes here"
last_modified: "2022-11-13"
---
> Last modified: {% raw  %}{{ page.last_modified | date: "%Y-%m-%d"}}{% endraw  %}
```
What we are doing is setting a text string that can be accessed via `page.last_modified` and then it is converted into whatever format we dictate to the right of the pipe (`date: "%Y-%m-%d"`).

5. **Do not create front matter for the landing documentation page -> `docs/README.md`**


### Example markdown file
The page that you are currently reading is named `documentation.md` within the GitHub repository and begins in the following way:

```
---
title: "Updating this documentation"
last_modified: "2022-11-13"
---
> Last modified: {% raw  %}{{ page.last_modified | date: "%Y-%m-%d"}}{% endraw  %}

The documentation on this site is rendered automatically by [GitHub Pages](https://docs.github.com/en/pages/getting-started-with-github-pages/creating-a-github-pages-site).

**Adding a new page to this documentation is as simple as creating a [markdown file](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet) (extension `.md`).**

Important things to remember: ...
```

