package com.database.atypon.DBMS.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Form-login security for the Thymeleaf gateway. Authentication is delegated to the cluster via
 * {@link NodeAuthenticationProvider}; on success the node-issued JWT and node URL are stashed in the
 * HTTP session so the existing controllers keep reading {@code session["token"]} / {@code ["nodeURL"]}
 * and forward the JWT as a Bearer token. Replaces the former custom AuthInterceptor. The Actuator
 * {@code /actuator/health} and {@code /actuator/prometheus} endpoints are public so Prometheus can scrape without a login.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    private final NodeAuthenticationProvider provider;

    public SecurityConfig(NodeAuthenticationProvider provider) {
        this.provider = provider;
    }

    @Bean
    public SecurityFilterChain chain(HttpSecurity http) throws Exception {
        http.csrf().disable()
                .authenticationProvider(provider)
                .authorizeRequests()
                .antMatchers("/login", "/css/**", "/js/**", "/webjars/**").permitAll()
                // Prometheus scrape + liveness probe: no login (the only Actuator endpoints exposed, see the app module)
                .antMatchers("/actuator/health", "/actuator/prometheus").permitAll()
                .anyRequest().authenticated()
                .and()
                .formLogin()
                .loginPage("/login")
                .loginProcessingUrl("/login")
                .successHandler((request, response, authentication) -> {
                    DbmsUser u = (DbmsUser) authentication.getPrincipal();
                    request.getSession().setAttribute("token", u.getJwt());
                    request.getSession().setAttribute("nodeURL", u.getNodeURL());
                    request.getSession().setAttribute("username", u.getUsername());
                    response.sendRedirect("/dashboard");
                })
                .failureUrl("/login?error")
                .and()
                .logout().logoutUrl("/logout").logoutSuccessUrl("/login");
        return http.build();
    }
}
