package com.database.atypon.BootstrappingNode.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * Stateless JWT security for the bootstrapping node. {@code /getUserNode} and {@code /test} are
 * public ({@code getUserNode} runs during the login handshake, before the user holds a token);
 * creating users requires ADMIN, and mesh wiring ({@code /network/**}) and the SWIM seed
 * ({@code /swim/**}, where data nodes join the cluster) require INTERNAL.
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
                .antMatchers("/test", "/getUserNode").permitAll()
                .antMatchers("/createNewUser").hasRole("ADMIN")
                .antMatchers("/network/**").hasRole("INTERNAL")
                .antMatchers("/swim/**").hasRole("INTERNAL")
                .anyRequest().authenticated()
                .and()
                .addFilterBefore(jwtAuthFilter, UsernamePasswordAuthenticationFilter.class);
        return http.build();
    }
}
