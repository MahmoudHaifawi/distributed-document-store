package com.database.atypon.Node.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * Stateless JWT resource-server security for the data node. The {@link JwtAuthFilter} authenticates
 * from the Bearer token; these URL rules enforce the role hierarchy (INTERNAL ⊇ ADMIN ⊇ USER).
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http, JwtService jwtService) throws Exception {
        JwtAuthFilter jwtAuthFilter = new JwtAuthFilter(jwtService);
        http.csrf().disable()
                .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)
                .and()
                .authorizeRequests()
                .antMatchers("/login", "/health").permitAll()
                .antMatchers("/network/**").hasRole("INTERNAL")
                // mvcMatchers, unlike antMatchers, matches the URL variants Spring MVC serves for this handler
                // (such as a trailing slash); the handler also checks the role itself
                .mvcMatchers("/write/document/repair").hasRole("INTERNAL")
                .antMatchers("/admin/**").hasRole("ADMIN")
                .antMatchers("/user/**", "/write/**").hasRole("USER")
                .anyRequest().authenticated()
                .and()
                .addFilterBefore(jwtAuthFilter, UsernamePasswordAuthenticationFilter.class);
        return http.build();
    }
}
