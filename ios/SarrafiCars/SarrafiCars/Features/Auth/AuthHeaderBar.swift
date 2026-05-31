import SwiftUI

struct AuthHeaderBar: View {
    var body: some View {
        AccountHeaderButtons(variant: .lightBackground)
    }
}

enum AuthHeaderVariant {
    case navyBar
    case lightBackground
}

struct AccountHeaderButtons: View {
    var variant: AuthHeaderVariant = .lightBackground
    @EnvironmentObject private var appState: AppState
    @State private var showLogin = false
    @State private var showRegister = false
    @State private var showProfile = false

    var body: some View {
        Group {
            if appState.isLoggedIn {
                Menu {
                    if let email = appState.user?.email, !email.isEmpty {
                        Text(email)
                    }
                    Button("Dashboard") { showProfile = true }
                    if !appState.hasPremiumAccess {
                        Link("Premium", destination: AppConfig.baseURL.appending(path: "premium"))
                    }
                    Button("Sign out", role: .destructive) {
                        Task { await appState.logout() }
                    }
                } label: {
                    Label("Profile", systemImage: "person.crop.circle.fill")
                        .font(.title3)
                        .foregroundStyle(variant == .navyBar ? AppTheme.cream : AppTheme.navy)
                }
            } else {
                HStack(spacing: 16) {
                    Button("Sign in") { showLogin = true }
                        .font(.subheadline.weight(.medium))
                        .foregroundStyle(variant == .navyBar ? AppTheme.cream.opacity(0.9) : AppTheme.navy)
                    Button("Get started") { showRegister = true }
                        .font(.subheadline.weight(.medium))
                        .foregroundStyle(variant == .navyBar ? AppTheme.cream : AppTheme.navy)
                        .padding(.horizontal, variant == .navyBar ? 14 : 12)
                        .padding(.vertical, 7)
                        .overlay(
                            Capsule()
                                .stroke(
                                    variant == .navyBar ? AppTheme.cream.opacity(0.45) : AppTheme.navy,
                                    lineWidth: 1
                                )
                        )
                }
            }
        }
        .sheet(isPresented: $showLogin) {
            LoginSheet()
                .environmentObject(appState)
        }
        .sheet(isPresented: $showRegister) {
            RegisterSheet()
                .environmentObject(appState)
        }
        .sheet(isPresented: $showProfile) {
            WebSheetView(path: "dashboard", title: "Dashboard")
        }
    }
}

struct LoginSheet: View {
    @EnvironmentObject private var appState: AppState
    @Environment(\.dismiss) private var dismiss
    @State private var login = ""
    @State private var password = ""

    var body: some View {
        NavigationStack {
            Form {
                TextField("Email or username", text: $login)
                    .textContentType(.username)
                    .autocapitalization(.none)
                SecureField("Password", text: $password)
                if let err = appState.authError {
                    Text(err).foregroundStyle(.red).font(.caption)
                }
            }
            .navigationTitle("Sign in")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Sign in") {
                        Task {
                            if await appState.login(login: login, password: password) {
                                dismiss()
                            }
                        }
                    }
                }
            }
        }
    }
}

struct RegisterSheet: View {
    @EnvironmentObject private var appState: AppState
    @Environment(\.dismiss) private var dismiss
    @State private var username = ""
    @State private var email = ""
    @State private var password = ""

    var body: some View {
        NavigationStack {
            Form {
                TextField("Username", text: $username)
                    .textContentType(.username)
                    .autocapitalization(.none)
                TextField("Email", text: $email)
                    .textContentType(.emailAddress)
                    .keyboardType(.emailAddress)
                    .autocapitalization(.none)
                SecureField("Password", text: $password)
                    .textContentType(.newPassword)
                if let err = appState.authError {
                    Text(err).foregroundStyle(.red).font(.caption)
                }
            }
            .navigationTitle("Get started")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Create account") {
                        Task {
                            if await appState.register(
                                username: username,
                                email: email,
                                password: password
                            ) {
                                dismiss()
                            }
                        }
                    }
                }
            }
        }
    }
}

struct WebSheetView: View {
    let path: String
    let title: String
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            EmbeddedWebView(url: AppConfig.baseURL.appending(path: path))
                .navigationTitle(title)
                .toolbar {
                    ToolbarItem(placement: .confirmationAction) {
                        Button("Done") { dismiss() }
                    }
                }
        }
    }
}
