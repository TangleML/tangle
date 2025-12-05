# **Project Tangle Governance**

## **Tangle Software Vision**

The Tangle system helps users create and run ML experiments and production pipelines. Any batch workflow that has beginning and end can be orchestrated via a pipeline.

## **Tangle Project Governance Vision**

Grow the project in a manner that welcomes a wide community of stakeholders and enables the future handoff to a vendor-neutral non-profit foundation.

## **Purpose and scope**

* This document outlines the governance model for the Tangle project, including how decisions are made and how various community members can get involved.
* Applies to all repositories under https://github.com/TangleML and affiliated tooling, documentation, and community channels.

## **Values**

* Transparency: Decisions and rationales are public and recorded.
* Inclusivity and neutrality: Multiple organizations and independent contributors can participate meaningfully.
* Security and quality: Changes meet defined standards and guardrails.
* Sustainability: Reduce single-maintainer/vendor risk and build a healthy maintainer pipeline.

## **A three-pillar governance model**

1. Alexey Volkov @Ark-kun \- founder and Project Lead.
2. Open Source Community \- can contribute code, become maintainers and join Technical Steering Committee (TSC) once it is formed.
3. Shopify \- initial sponsor, administrator of GitHub & security response, owner of domains and trademarks.

## **Project structure**

* License: Project consists of Open Source Software licensed under the Apache License, version 2.0 in `LICENSE`.
* Components:
  * [tangle](https://github.com/TangleML/tangle) \- Tangle is a web app that allows the users to build and run Machine Learning pipelines without having to set up a development environment.
  * [tangle-ui](https://github.com/TangleML/tangle-ui) \- Tangle frontend code \- based on Cloud Pipelines Studio
  * [website](https://github.com/TangleML/website) \- The source code for [TangleML.com](https://TangleML.com)
* Releases:
  * [https://github.com/TangleML/tangle/releases](https://github.com/TangleML/tangle/releases)
  * [https://github.com/TangleML/tangle-ui/releases](https://github.com/TangleML/tangle-ui/releases)
  * [https://huggingface.co/TangleML](https://huggingface.co/TangleML)
  * PyPI (details TBD)
* Documents: `LICENSE`, `CONTRIBUTING.md`, `SECURITY.md`, `RELEASING.md`, `TRADEMARK.pdf`

## **1\. Roles**

This project formally recognizes the following roles.

### **👤 Everyone (Users)**

Anyone who uses the project is a community member. We value your contributions, and you are encouraged to:

* Use the software.
* Provide feedback and report bugs by opening Issues.
* Join community discussions (e.g., on GitHub, Slack, or mailing lists).
* Advocate for the project.

### **🧑‍💻 Contributors**

A **Contributor** is anyone who makes a contribution to the project. Contributions can include:

* Submitting code via a Pull Request (PR).
* Improving documentation.
* Reviewing PRs from other contributors.
* Triaging issues and helping other users.

Contributors are expected to follow the project's [Contributing Guidelines](https://github.com/TangleML/tangle/blob/master/CONTRIBUTING.md).

### **🧑‍🔧 Maintainers**

**Maintainers** are active and trusted contributors who have demonstrated a long-term commitment to the project. They have **write access** to the repository and are responsible for its day-to-day health. More details on how to become a project maintainer are listed below.

**Responsibilities:**

* Reviewing and merging Pull Requests.
* Guiding the project's technical direction.
* Triaging and managing issues.
* Helping new contributors.

The current list of Maintainers is:

* *tangle*
  * @Ark-kun
* *tangle-ui*
  * @Ark-kun
  * @camielvs
  * @maxy-shpfy
  * @Mbeaulne
* *website*
  * @Ark-kun
  * @maxy-shpfy

### **🚀 Release Managers**

* Coordinate releases, tags, changelogs, artifact signing, and backports.
* Operate per `RELEASING.md`.
* The Release Managers is a subset of the maintainers that are assigned additional system permissions to perform the release process. (for example to publish to PyPi)

### **🛡️ Security Team**

* Receives and triages private vulnerability reports; coordinates fixes and advisories.
* Operates per `SECURITY.md`; may include external members approved by the Project Lead or TSC.

### **👑 Project Lead**

For a new project, transparency is key. Tangle has been started with a Project Lead to drive the project. The project and Project Lead are supported by Shopify.

The **Project Lead** is responsible for the overall strategic vision of the project and for making final decisions when community consensus cannot be reached.

* **Project Lead:** Alexey Volkov @Ark-kun

As the project grows, this role will be retired and transferred to a Technical Steering Committee.

### **🏛️ Technical Steering Committee (TSC)**

The Technical Steering Committee will grow as an organizationally diverse set of maintainers is achieved. Once the size of the TSC reaches 10, the Project Lead role will become an elected role with a 1 year term. The Project Lead chairs the TSC and can appoint new members from the current Maintainers. Once the TSC has 5 members, voting will be introduced to elect new members for 1 year terms.

* Size: 3-10 members, growing with the size of the project. Composition aims for diversity across organizations and expertise.
* Responsibilities: Strategy, roadmap approval, governance changes, tie-break decisions, high-impact/controversial changes.
* Publishes meeting notes and decisions.
* Members of the TSC are a subset of the project Maintainers chosen to represent different community stakeholders. (To retain their TSC seat, TSC members must maintain active maintainer status)

As a reflection of the three pillars vision, 3 seats on the TSC will represent the 3 pillars of Community, Shopify and the Project Leader.

## **2\. Decision Making**

This project operates on a **consensus-seeking** model. We try to find solutions that most members can agree with.

### **Day-to-Day Decisions (Pull Requests)**

1. **Pull Requests** are the primary method for making changes.
2. PRs should be reviewed by at least one Maintainer who is not the author.
3. If a PR is straightforward (like a bug fix or documentation) and receives an approval, it can be merged by a Maintainer.
4. The author of a PR should generally not merge their own PR unless it's a critical fix or they have received explicit approval from another Maintainer.

### **Major Changes & Disagreements**

For substantial changes (e.g., new features, API changes, or changes to the project's direction), a more formal process is used:

1. **Proposal:** The change is proposed by opening a **GitHub Issue** with a clear description.
2. **Discussion:** The community discusses the proposal. This is the time to raise concerns, suggest alternatives, and build consensus.
3. **Resolution:**
   * **Consensus:** If the repository’s Maintainers agree, a Maintainer can mark the proposal as "accepted," and work can begin.
   * **No Consensus:** If a clear consensus cannot be reached the matter will be escalated to the TSC.

It is expected that a more formal RFC process is instituted for major changes as the project grows. This will be instituted by the Project Lead or Technical Steering Committee and this document updated via the processes in the current version.

## **3\. How to Become a Maintainer**

We actively want our best contributors to become Maintainers. The path is simple: **contribute consistently and help others.**

A new maintainer can be nominated by a current maintainer. Maintainers are voted in by a simple majority among current maintainers. Eligibility to become a maintainer is based on active contribution and volume of at least 30 merged pull requests in the last year. A different set of maintainers may be maintained for each repository (e.g website, tangle, tangleUI). A subset of maintainers will have organization wide permissions and responsibility.  Maintainers can lose their status if they haven’t contributed (code, discussion participation, other duties) in more than one year.

## **4\. Amending This Document**

This governance document is not set in stone. Proposed changes shall be made public for 30 days and decisions will be made by the TSC.

## **5\. Project Infrastructure**

Project infrastructure such as GitHub, CI, Hugging Face, PyPi, etc. are administered by the TSC with access permissions assigned to Maintainers, Release Managers, and Security Team members in accordance with this document.

## **6\. Ownership**

Shopify is the owner of the Tangle name, Trademark, Logo and Domain. In the event of Shopify losing interest in developing and maintaining the project, Shopify plans to donate these assets to a suitable steward. It is intended that that suitable steward might be an appropriate vendor-neutral non-profit open source foundation (for example, CNCF).

## **Contact**

* General questions: (TBD: Google Groups? Slack?)
* Security reports: [https://github.com/TangleML/tangle/security](https://github.com/TangleML/tangle/security)

