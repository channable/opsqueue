## State transitions

For changes to behavior described by a state diagram, or introducing new states
or transitions:
- Check that all implemented transitions and their triggers are documented in
  the relevant state diagram.
- Check that each transition has a test exercising its actual trigger and
  asserting the resulting state. Different triggers between the same states
  need separate coverage.
- Flag missing tests or diagram updates related to the PR.
