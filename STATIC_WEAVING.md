# Static Weaving Configuration

This document explains the static weaving setup for JPA entities in the Fineract project.

## Overview

Static weaving is a process that enhances JPA entities at build time to improve runtime performance. This is done using the `org.eclipse.persistence.tools.weaving.jpa.StaticWeave` which processes the compiled classes and applies the necessary bytecode transformations.

## Configuration

The static weaving is configured in `static-weaving.gradle` and applied to all Java projects that contain JPA managed classes.

## How It Works

1. **Compilation**: Java source files are compiled to the standard classes directory (`build/classes/java/main`).
2. **Persistence metadata generation**: The build scans the current module and its transitive project runtime dependencies for JPA entities, mapped superclasses, and converters. It generates `build/tmp/compileJava/static-weaving/META-INF/persistence.xml`.
3. **Weaving**: Weaving happens as the last step of the **compileJava** task, which outputs enhanced classes to the standard classes directory (`build/classes/java/main`).

## Adding Static Weaving to a Module

1. Add JPA entities to `src/main/java`
2. Add project dependencies normally in Gradle
3. The build will automatically detect and apply static weaving

Do not add module-level static-weaving `persistence.xml` files under `src/main/resources/jpa/static-weaving`; the build generates the weaving metadata.

## Troubleshooting

If you encounter issues with static weaving:

1. Check the generated `build/tmp/compileJava/static-weaving/META-INF/persistence.xml`
2. Verify that the output directories are being created correctly
3. Check the build logs for any weaving-related errors
