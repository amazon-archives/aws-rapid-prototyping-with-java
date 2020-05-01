package com.amazon.aws.prototyping;

import software.amazon.awscdk.core.App;

public class CdkApp {
    public static void main(final String[] args) {
        App app = new App();

        new CdkStack(app, "JavaSamplesCdkStack");

        app.synth();
    }
}
