# frozen_string_literal: true

module GhostferryDocs
  # Renders the repository's root CHANGELOG.md as the body of docs/changelog.md,
  # so the release history is maintained in one place. Runs after
  # jekyll-optional-front-matter (:normal) has loaded the page and before
  # jekyll-relative-links (:lowest) rewrites links.
  class ChangelogGenerator < Jekyll::Generator
    priority :low

    def generate(site)
      page = site.pages.find { |candidate| candidate.relative_path == "changelog.md" }
      raise Jekyll::Errors::FatalException, "Changelog page docs/changelog.md was not loaded" unless page

      page.content = File.read(File.expand_path("../CHANGELOG.md", site.source), encoding: "UTF-8")
      # Makes the theme's "Edit this page" link point at the root CHANGELOG.md.
      page.data["path"] = "../CHANGELOG.md"
    end
  end
end
