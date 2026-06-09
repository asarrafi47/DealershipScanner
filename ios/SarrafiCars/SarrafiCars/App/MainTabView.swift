import SwiftUI

private enum AppTab: Hashable {
    case home
    case saved
    case dealers
    case premium
}

struct MainTabView: View {
    @StateObject private var appState = AppState()
    @State private var tab: AppTab = .home

    var body: some View {
        TabView(selection: $tab) {
            HomeView()
                .tabItem { Label("Home", systemImage: "house.fill") }
                .tag(AppTab.home)

            if appState.isLoggedIn {
                SavedCarsView()
                    .tabItem { Label("Saved", systemImage: "heart.fill") }
                    .tag(AppTab.saved)
            }

            DealersView()
                .tabItem { Label("Dealers", systemImage: "mappin.and.ellipse") }
                .tag(AppTab.dealers)

            if !appState.hasPremiumAccess {
                PremiumWebView()
                    .tabItem { Label("Premium", systemImage: "star.fill") }
                    .tag(AppTab.premium)
            }
        }
        .tint(AppTheme.navy)
        .task { await appState.bootstrap() }
        .environmentObject(appState)
        .onChange(of: appState.isLoggedIn) { loggedIn in
            if !loggedIn, tab == .saved {
                tab = .home
            }
        }
        .onChange(of: appState.hasPremiumAccess) { hasPremium in
            if hasPremium, tab == .premium {
                tab = .home
            }
        }
    }
}

#Preview {
    MainTabView()
}
