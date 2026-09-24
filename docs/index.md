<a name="welcome-to-ghostferry-s-documentation"></a>

# Welcome to Ghostferry's documentation!

Contents:

- [Introduction to Ghostferry](introduction.md)
    - [Why do I need this?](introduction.md#why-do-i-need-this)
- [Technical Overview](technicaloverview.md)
    - [Architecture](technicaloverview.md#architecture)
    - [Limitations](technicaloverview.md#limitations)
    - [Algorithm Correctness](technicaloverview.md#algorithm-correctness)
- [Tutorial for ghostferry-copydb](tutorialcopydb.md)
    - [Setup and Seed MySQL](tutorialcopydb.md#setup-and-seed-mysql)
    - [(Mirrors Production) Create Ghostferry Users](tutorialcopydb.md#mirrors-production-create-ghostferry-users)
    - [(Mirrors Production) Install ghostferry-copydb](tutorialcopydb.md#mirrors-production-install-ghostferry-copydb)
    - [(Mirrors Production) Setup Ghostferry Run Configuration](tutorialcopydb.md#mirrors-production-setup-ghostferry-run-configuration)
    - [(Mirrors Production) Validate Ghostferry Configuration](tutorialcopydb.md#mirrors-production-validate-ghostferry-configuration)
    - [(Mirrors Production) Starting Ghostferry Run](tutorialcopydb.md#mirrors-production-starting-ghostferry-run)
    - [(Mirrors Production) Monitoring Ghostferry Run via Web UI](tutorialcopydb.md#mirrors-production-monitoring-ghostferry-run-via-web-ui)
    - [(Mirrors Production) Perform Cutover](tutorialcopydb.md#mirrors-production-perform-cutover)
    - [(Mirrors Production) Verify Source and Target Data are Identical](tutorialcopydb.md#mirrors-production-verify-source-and-target-data-are-identical)
    - [Finishing Ghostferry Run and Next Steps](tutorialcopydb.md#finishing-ghostferry-run-and-next-steps)
- [Running `ghostferry-copydb` in production](copydbinprod.md)
    - [Prerequisites](copydbinprod.md#prerequisites)
    - [Testing Ghostferry with Production Data](copydbinprod.md#testing-ghostferry-with-production-data)
    - [To Verify Or Not To Verify](copydbinprod.md#to-verify-or-not-to-verify)
    - [Dealing with Errors and Restarting Runs](copydbinprod.md#dealing-with-errors-and-restarting-runs)
    - [Configuration for `ghostferry-copydb`](copydbinprod.md#configuration-for-ghostferry-copydb)
- [Interrupt and resuming `ghostferry-copydb`](copydbinterruptresume.md)
- [Verifiers](verifiers.md)
    - [IterativeVerifier (Deprecated)](verifiers.md#iterativeverifier-deprecated)
    - [InlineVerifier](verifiers.md#inlineverifier)
    - [TargetVerifier](verifiers.md#targetverifier)
- [Using Ghostferry in Custom Applications](howtousecustom.md)
    - [Consuming Ghostferry Metrics](howtousecustom.md#consuming-ghostferry-metrics)

## Other resources

- [API Documentations](https://godoc.org/github.com/Shopify/ghostferry)
- [**Percona Live Conference Slides + Presenter Notes**](_static/percona-talk.pdf)
