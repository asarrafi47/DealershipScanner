import SwiftUI

struct AccountTabView: View {
    @EnvironmentObject private var appState: AppState

    @State private var loginInput = ""
    @State private var password = ""
    @State private var isSubmitting = false

    var body: some View {
        NavigationStack {
            Form {
                if appState.isLoadingSession {
                    Section {
                        HStack {
                            Spacer()
                            ProgressView()
                            Spacer()
                        }
                    }
                } else if let user = appState.user {
                    signedInSection(user: user)
                } else {
                    signInSection
                }

                Section("Website") {
                    Link("Open sarraficars.com in Safari", destination: AppConfig.baseURL)
                    if appState.isLoggedIn {
                        Link("Upgrade to Premium", destination: AppConfig.baseURL.appending(path: "premium"))
                    }
                }
            }
            .navigationTitle("Account")
        }
    }

    @ViewBuilder
    private func signedInSection(user: AuthUser) -> some View {
        Section("Signed in") {
            if let email = user.email, !email.isEmpty {
                LabeledContent("Email", value: email)
            }
            if let username = user.username, !username.isEmpty {
                LabeledContent("Username", value: username)
            }
            if user.hasPaidAccess == true || user.isPremium == true {
                Label("Premium active", systemImage: "star.fill")
                    .foregroundStyle(.orange)
            }
        }
        Section {
            Button("Sign out", role: .destructive) {
                Task { await appState.logout() }
            }
        }
        if let err = appState.authError {
            Section {
                Text(err).foregroundStyle(.red).font(.caption)
            }
        }
    }

    private var signInSection: some View {
        Group {
            Section("Sign in") {
                TextField("Email or username", text: $loginInput)
                    .textContentType(.username)
                    .autocapitalization(.none)
                    .autocorrectionDisabled()
                SecureField("Password", text: $password)
                    .textContentType(.password)
                Button {
                    Task { await submitLogin() }
                } label: {
                    if isSubmitting {
                        ProgressView()
                    } else {
                        Text("Sign in")
                    }
                }
                .disabled(isSubmitting || loginInput.isEmpty || password.isEmpty)
            }
            if let err = appState.authError {
                Section {
                    Text(err).foregroundStyle(.red).font(.caption)
                }
            }
        }
    }

    private func submitLogin() async {
        isSubmitting = true
        defer { isSubmitting = false }
        let ok = await appState.login(login: loginInput, password: password)
        if ok {
            password = ""
        }
    }
}
