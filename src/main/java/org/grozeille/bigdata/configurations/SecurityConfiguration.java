package org.grozeille.bigdata.configurations;

import org.grozeille.bigdata.repositories.jpa.AdminUserRepository;
import org.grozeille.bigdata.repositories.jpa.UserRepository;
import org.grozeille.bigdata.resources.admin.model.AdminUser;
import org.grozeille.bigdata.resources.user.model.User;
import org.grozeille.bigdata.services.InternalAuthoritiesExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.security.oauth2.client.OAuth2SsoProperties;
import org.springframework.boot.autoconfigure.security.oauth2.resource.AuthoritiesExtractor;
import org.springframework.boot.autoconfigure.security.oauth2.resource.ResourceServerProperties;
import org.springframework.boot.autoconfigure.security.oauth2.resource.UserInfoTokenServices;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.client.OAuth2ClientContext;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.client.filter.OAuth2ClientAuthenticationProcessingFilter;
import org.springframework.security.oauth2.client.filter.OAuth2ClientContextFilter;
import org.springframework.security.oauth2.client.token.grant.code.AuthorizationCodeResourceDetails;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

import javax.servlet.Filter;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class SecurityConfiguration extends WebSecurityConfigurerAdapter {

    @Autowired
    private OAuth2ClientContext oauth2ClientContext;

    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

    @Autowired
    private InternalAuthoritiesExtractor internalAuthoritiesExtractor;

    @Value("${security.enabled}")
    private boolean securityEnabled;

    @Bean
    @ConfigurationProperties("security.oauth2.client")
    public AuthorizationCodeResourceDetails client() {
        return new AuthorizationCodeResourceDetails();
    }

    @Bean
    @ConfigurationProperties("security.oauth2.resource")
    public ResourceServerProperties resource() {
        return new ResourceServerProperties();
    }

    @Bean
    public OAuth2SsoProperties oAuth2SsoProperties(){
        return new OAuth2SsoProperties();
    }

    @Bean
    public FilterRegistrationBean oauth2ClientFilterRegistration(OAuth2ClientContextFilter filter) {
        FilterRegistrationBean registration = new FilterRegistrationBean();
        registration.setFilter(filter);
        registration.setOrder(-100);
        return registration;
    }

    private Filter ssoFilter() {
        OAuth2ClientAuthenticationProcessingFilter filter = new OAuth2ClientAuthenticationProcessingFilter("/login");

        filter.setApplicationEventPublisher(this.applicationEventPublisher);

        UserInfoTokenServices userInfoTokenServices = new UserInfoTokenServices(resource().getUserInfoUri(), resource().getClientId());
        userInfoTokenServices.setAuthoritiesExtractor(internalAuthoritiesExtractor);
        filter.setTokenServices(userInfoTokenServices);

        OAuth2RestTemplate template = new OAuth2RestTemplate(client(), oauth2ClientContext);
        filter.setRestTemplate(template);

        return filter;
    }

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        if(securityEnabled) {
            super.configure(auth);
        }
        else {
            auth.inMemoryAuthentication()
                    .withUser("admin")
                    .password("admin")
                    .authorities("ROLE_USER", "ROLE_ADMIN")
            .and()
                    .withUser("user")
                    .password("user")
                    .authorities("ROLE_USER");
        }
    }

    @Override
    public void configure(WebSecurity web) throws Exception {
        web.ignoring()
                .antMatchers(
                        "/**/*.css", "/**/*.js",
                        "/**/*.ttf", "/**/*.eot", "/**/*.woff", "/**/*.woff2","/**/*.svg",
                        "/**/*.png", "/**/*.jpg", "/**/*.gif");
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        if(securityEnabled) {
            http
                    .authorizeRequests()
                        .antMatchers(
                            "/login**",
                            "/login",
                            "/health**",
                            "/info**",
                            "/metrics**",
                            "/h2-console/**",
                            "/api/profile/user"
                        ).permitAll()
                        .antMatchers("/").authenticated()
                        .antMatchers("/**").hasRole("USER")
                        .anyRequest().authenticated()
                    .and()
                    .exceptionHandling().authenticationEntryPoint(new LoginUrlAuthenticationEntryPoint("/login"))
                    .and()
                    .logout().logoutUrl("/logout").logoutSuccessUrl("/").permitAll()
                    .and()
                    .headers().frameOptions().disable()
                    .and()
                    .csrf().disable()
                    .addFilterBefore(ssoFilter(), BasicAuthenticationFilter.class);
        }
        else {
            //http.antMatcher("/**").authorizeRequests().anyRequest().permitAll().and().csrf().disable();

            http
                    .authorizeRequests()
                    .antMatchers(
                            "/login**",
                            "/login",
                            "/health**",
                            "/info**",
                            "/metrics**",
                            "/h2-console/**",
                            "/api/profile/user"
                    ).permitAll()
                    .antMatchers("/").authenticated()
                    .antMatchers("/**").hasRole("USER")
                    .anyRequest().authenticated()
                    .and()
                    .headers().frameOptions().disable()
                    .and()
                    .httpBasic();
        }
    }
}
